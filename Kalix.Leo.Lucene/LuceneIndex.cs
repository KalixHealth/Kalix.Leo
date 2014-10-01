using Kalix.Leo.Encryption;
using Kalix.Leo.Lucene.Analysis;
using Kalix.Leo.Lucene.Store;
using Kalix.Leo.Storage;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using IO = System.IO;

namespace Kalix.Leo.Lucene
{
    public class LuceneIndex : IDisposable, ILuceneIndex
    {
        private readonly Directory _directory;
        private readonly Analyzer _analyzer;

        private readonly Directory _cacheDirectory;

        // Only one index writer per lucene index, however all the writing happens on the single write thread
        private readonly Lazy<IndexWriter> _writer;
        private object _writeLock = new object();
        private Task _writeThread = Task.FromResult(0);

        private int _readerRefreshRate;
        private IndexReader _reader;
        private DateTime _lastRefresh; // We use this to refresh every n secs or so
        private object _readerLock = new object();

        private double _RAMSizeMb;
        private bool _isDisposed;

        private static readonly string _baseDirectory = IO.Path.Combine(IO.Path.GetTempPath(), IO.Path.GetRandomFileName());

        /// <summary>
        /// Create a lucene index over the top of a secure store, using an encrypted file cache and english analyzer
        /// Only one instance should be used for both indexing and searching (on any number of threads) for best results
        /// </summary>
        /// <param name="store">Store to have the Indexer on top of</param>
        /// <param name="container">Container to put the index</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        /// <param name="basePath">The path to namespace this index in</param>
        /// <param name="encryptor">The encryptor to encryt any records being saved</param>
        /// <param name="secsTillReaderRefresh">This is the amount of time to cache the reader before updating it</param>
        public LuceneIndex(ISecureStore store, string container, string basePath, IEncryptor encryptor, double RAMSizeMb = 20, int secsTillReaderRefresh = 10)
        {
            _cacheDirectory = new RAMDirectory();
            _directory = new SecureStoreDirectory(_cacheDirectory, store, container, basePath, encryptor);
            _analyzer = new EnglishAnalyzer();
            _RAMSizeMb = RAMSizeMb;
            _readerRefreshRate = secsTillReaderRefresh;

            _writer = new Lazy<IndexWriter>(InitWriter);
        }

        /// <summary>
        /// Lower level constructor, put in your own cache, (lucene) directory and (lucene) analyzer
        /// </summary>
        /// <param name="directory">Lucene directory of your files</param>
        /// <param name="analyzer">Analyzer you want to use for your indexing/searching</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        /// <param name="secsTillReaderRefresh">This is the amount of time to cache the reader before updating it</param>
        public LuceneIndex(Directory directory, Analyzer analyzer, double RAMSizeMb = 20, int secsTillReaderRefresh = 10)
        {
            _directory = directory;
            _analyzer = analyzer;
            _RAMSizeMb = RAMSizeMb;
            _readerRefreshRate = secsTillReaderRefresh;

            _writer = new Lazy<IndexWriter>(InitWriter);
        }

        public Task WriteToIndex(IObservable<Document> documents)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            lock(_writeLock)
            {
                return _writeThread = _writeThread.ContinueWith(async (t) =>
                {
                    await documents
                        .Do(_writer.Value.AddDocument)
                        .LastOrDefaultAsync();

                    _writer.Value.Commit();
                }).Unwrap();
            }
        }

        public Task WriteToIndex(Action<IndexWriter> writeUsingIndex)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            lock (_writeLock)
            {
                return _writeThread = _writeThread.ContinueWith((t) =>
                {
                    writeUsingIndex(_writer.Value);
                    _writer.Value.Commit();
                });
            }
        }

        public IObservable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            return SearchDocuments((s, a) => doSearchFunc(s));
        }

        public IObservable<Document> SearchDocuments(Func<IndexSearcher, Analyzer, TopDocs> doSearchFunc)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            return Observable.Create<Document>(obs =>
            {
                var cts = new CancellationTokenSource();
                var token = cts.Token;

                Task.Run(() =>
                {
                    try
                    {
                        using(var reader = Reader())
                        using (var searcher = new IndexSearcher(reader))
                        {
                            var docs = doSearchFunc(searcher, _analyzer);

                            foreach (var doc in docs.ScoreDocs)
                            {
                                obs.OnNext(searcher.Doc(doc.Doc));
                                token.ThrowIfCancellationRequested();
                            }
                        }

                        obs.OnCompleted();
                    }
                    catch (Exception e)
                    {
                        obs.OnError(e);
                    }
                }, token);

                return cts;
            });
        }

        public Task DeleteAll()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            lock (_writeLock)
            {
                return _writeThread = _writeThread.ContinueWith((t) =>
                {
                    _writer.Value.DeleteAll();
                    _writer.Value.Commit();
                });
            }
        }

        private IndexWriter InitWriter()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            IndexWriter writer;
            try
            {
                writer = new IndexWriter(_directory, _analyzer, false, IndexWriter.MaxFieldLength.UNLIMITED);
            }
            catch(System.IO.FileNotFoundException)
            {
                writer = new IndexWriter(_directory, _analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);
            }
            writer.UseCompoundFile = false;
            writer.SetRAMBufferSizeMB(_RAMSizeMb);
            writer.MergeFactor = 10;
            return writer;
        }

        private IndexReader Reader()
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            if(_reader == null || _lastRefresh < DateTime.UtcNow)
            {
                lock(_readerLock)
                {
                    if (_reader == null)
                    {
                        try
                        {
                            _reader = IndexReader.Open(_directory, true);
                        }
                        catch (System.IO.FileNotFoundException)
                        {
                            // Fire up the index if it doesnt exist yet!
                            var writer = new IndexWriter(_directory, _analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);
                            writer.Dispose();

                            _reader = IndexReader.Open(_directory, true);
                        }

                        _lastRefresh = DateTime.UtcNow.Add(TimeSpan.FromSeconds(_readerRefreshRate));
                    }
                    else if(_lastRefresh < DateTime.UtcNow)
                    {
                        var newReader = _reader.Reopen();
                        if(newReader != _reader)
                        {
                            _reader.Dispose();
                            _reader = newReader;
                        }

                        _lastRefresh = DateTime.UtcNow.Add(TimeSpan.FromSeconds(_readerRefreshRate));
                    }
                }
            }

            // Always return a disposable instance...
            return _reader.Clone(true);
        }

        public void Dispose()
        {
            if(!_isDisposed)
            {
                _isDisposed = true;

                if(_reader != null)
                {
                    _reader.Dispose();
                }

                if(_writer.IsValueCreated)
                {
                    _writer.Value.Dispose();
                }

                _analyzer.Dispose();
                _directory.Dispose();

                if(_cacheDirectory != null)
                {
                    _cacheDirectory.Dispose();
                }
            }
        }
    }
}
