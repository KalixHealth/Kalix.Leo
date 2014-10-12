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
        private IndexWriter _writer;
        private IndexReader _reader;
        private object _writeLock = new object();
        private Task _writeThread = Task.FromResult(0);

        private readonly double _RAMSizeMb;
        private readonly int _secsTillSearcherRefresh;
        private bool _isDisposed;

        private static readonly string _baseDirectory = IO.Path.Combine(IO.Path.GetTempPath(), "LeoLuceneIndexes");

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
            var path = IO.Path.Combine(_baseDirectory, container, basePath);
            _cacheDirectory = FSDirectory.Open(path);
            _directory = new SecureStoreDirectory(_cacheDirectory, store, container, basePath, encryptor);
            _analyzer = new EnglishAnalyzer();
            _RAMSizeMb = RAMSizeMb;
            _secsTillSearcherRefresh = secsTillReaderRefresh;
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
            _secsTillSearcherRefresh = secsTillReaderRefresh;
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
                    try
                    {
                        var writer = GetWriter();
                        await documents
                            .Do(writer.AddDocument)
                            .LastOrDefaultAsync();

                        writer.Commit();
                    }
                    catch (Exception)
                    {
                        ResetReaderWriter();
                        throw;
                    }
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
                    try
                    {
                        var writer = GetWriter();
                        writeUsingIndex(writer);
                        writer.Commit();
                    }
                    catch (Exception)
                    {
                        ResetReaderWriter();
                        throw;
                    }
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
                        using (var searcher = new IndexSearcher(GetReader()))
                        {
                            var docs = doSearchFunc(searcher, _analyzer);

                            foreach (var doc in docs.ScoreDocs)
                            {
                                obs.OnNext(searcher.Doc(doc.Doc));
                                token.ThrowIfCancellationRequested();
                            }

                            obs.OnCompleted();
                        }
                    }
                    catch (Exception e)
                    {
                        ResetReaderWriter();
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
                    try
                    {
                        var writer = GetWriter();
                        writer.DeleteAll();
                        writer.Commit();
                    }
                    catch(Exception)
                    {
                        ResetReaderWriter();
                        throw;
                    }
                });
            }
        }

        private IndexWriter GetWriter()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            if (_writer == null)
            {
                IndexWriter writer;
                try
                {
                    writer = new IndexWriter(_directory, _analyzer, false, IndexWriter.MaxFieldLength.UNLIMITED);
                }
                catch (System.IO.FileNotFoundException)
                {
                    writer = new IndexWriter(_directory, _analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);
                }
                writer.UseCompoundFile = false;
                writer.SetRAMBufferSizeMB(_RAMSizeMb);
                writer.MergeFactor = 10;
                _writer = writer;
            }

            return _writer;
        }

        private IndexReader GetReader()
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

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
            }

            return _reader;
        }

        private void ResetReaderWriter()
        {
            if (_reader != null)
            {
                _reader.Dispose();
                _reader = null;
            }

            if (_writer != null)
            {
                _writer.Dispose();
                _writer = null;
            }

            _writeThread = Task.FromResult(0);
        }

        public void Dispose()
        {
            if(!_isDisposed)
            {
                _isDisposed = true;
                ResetReaderWriter();

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
