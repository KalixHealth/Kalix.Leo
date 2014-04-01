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

        private int _readerRefreshRate;
        private IndexReader _reader;
        private DateTime _lastRefresh; // We use this to refresh every n secs or so
        private object _readerLock = new object();

        private double _RAMSizeMb;
        private bool _isDisposed;

        private static readonly string _baseDirectory = IO.Path.GetTempPath();

        /// <summary>
        /// Create a lucene index over the top of a secure store, using an encrypted file cache and english analyzer
        /// Only one instance should be used for both indexing and searching (on any number of threads) for best results
        /// </summary>
        /// <param name="store">Store to have the Indexer on top of</param>
        /// <param name="container">Container to put the index</param>
        /// <param name="readIndexThottleMs">The time before the read index can be refreshed, defaults to five seconds</param>
        /// <param name="writeIndexThottleMs">The interval to wait before writes</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        public LuceneIndex(ISecureStore store, string container, string basePath, IEncryptor encryptor, double RAMSizeMb = 20, int secsTillReaderRefresh = 10)
            : this(new SecureStoreDirectory(store, container, basePath, new EncryptedFileCache(_baseDirectory), encryptor), new EnglishAnalyzer(), RAMSizeMb, secsTillReaderRefresh)
        {
        }

        /// <summary>
        /// Lower level constructor, put in your own cache, (lucene) directory and (lucene) analyzer
        /// </summary>
        /// <param name="directory">Lucene directory of your files</param>
        /// <param name="analyzer">Analyzer you want to use for your indexing/searching</param>
        /// <param name="readIndexThottleMs">The time before the read index can be refreshed, defaults to five seconds</param>
        /// <param name="writeIndexThottleMs">The interval to wait before writes</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        public LuceneIndex(Directory directory, Analyzer analyzer, double RAMSizeMb = 20, int secsTillReaderRefresh = 10)
        {
            _directory = directory;
            _analyzer = analyzer;
            _RAMSizeMb = RAMSizeMb;
            _readerRefreshRate = secsTillReaderRefresh;
        }

        public async Task WriteToIndex(IObservable<Document> documents)
        {
            using (var writer = Writer())
            {
                await documents
                    .Do(writer.AddDocument)
                    .LastOrDefaultAsync();

                writer.Commit();
            }
        }

        public async Task WriteToIndex(Action<IndexWriter> writeUsingIndex)
        {
            await Task.Run(() =>
            {
                using (var writer = Writer())
                {
                    writeUsingIndex(writer);
                    writer.Commit();
                }
            }).ConfigureAwait(false);
        }

        public IObservable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc)
        {
            return SearchDocuments((s, a) => doSearchFunc(s));
        }

        public IObservable<Document> SearchDocuments(Func<IndexSearcher, Analyzer, TopDocs> doSearchFunc)
        {
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
            return Task.Run(() =>
            {
                using (var writer = Writer())
                {
                    writer.DeleteAll();
                    writer.Commit();
                }
            });
        }

        private IndexWriter Writer()
        {
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
                throw new ObjectDisposedException("Indexer");
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

                _analyzer.Dispose();
                _directory.Dispose();
            }
        }
    }
}
