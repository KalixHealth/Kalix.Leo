using Kalix.Leo.Lucene.Analysis;
using Kalix.Leo.Lucene.Store;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    public class LuceneIndexer : IDisposable, ILuceneIndexer
    {
        private readonly Directory _directory;
        private readonly Analyzer _analyzer;
        private readonly IDisposable _writeWatcher;
        private readonly int _readThrottleTimeMs;

        private double _RAMSizeMb;
        private Lazy<IndexWriter> _writer;
        private bool _needsWrite;
        private bool _isDisposed;

        /// <summary>
        /// Create a lucene index over the top of a secure store, using an encrypted file cache and english analyzer
        /// Only one instance should be used for both indexing and searching (on any number of threads) for best results
        /// </summary>
        /// <param name="store">Store to have the Indexer on top of</param>
        /// <param name="container">Container to put the index</param>
        /// <param name="readIndexThottleMs">The time before the read index can be refreshed, defaults to five seconds</param>
        /// <param name="writeIndexThottleMs">The interval to wait before writes</param>
        /// <param name="RAMSizeMb">The max amount of memory to use before flushing when writing</param>
        public LuceneIndexer(ISecureStore store, string container, int readIndexThottleMs = 5000, int writeIndexThottleMs = 30000, double RAMSizeMb = 20)
            : this(new SecureStoreDirectory(store, container, new EncryptedFileCache()), new EnglishAnalyzer(), readIndexThottleMs, writeIndexThottleMs, RAMSizeMb)
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
        public LuceneIndexer(Directory directory, Analyzer analyzer, int readIndexThottleMs = 5000, int writeIndexThottleMs = 30000, double RAMSizeMb = 20)
        {
            _directory = directory;
            _analyzer = analyzer;
            _readThrottleTimeMs = readIndexThottleMs;
            _RAMSizeMb = RAMSizeMb;

            _writeWatcher = Observable.Interval(TimeSpan.FromMilliseconds(writeIndexThottleMs))
                .Subscribe(i =>
                {
                    if (_needsWrite && _writer.IsValueCreated)
                    {
                        SafeWrite(() => _writer.Value.Commit());
                        _needsWrite = false;
                    }
                });

            RebuildWriter();
        }

        public async Task WriteToIndex(IObservable<Document> documents)
        {
            await documents
                .Do((d) =>
                {
                    SafeWrite(() => _writer.Value.AddDocument(d));
                },
                () =>
                {
                    SafeWrite(() => _writer.Value.PrepareCommit());
                    _needsWrite = true;
                })
                .LastOrDefaultAsync();   
        }

        public IObservable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc)
        {
            return Observable.Create<Document>(obs =>
            {
                var reader = GetThrottledReader();
                var searcher = new IndexSearcher(reader);
                var cts = new CancellationTokenSource();
                var token = cts.Token;

                Task.Run(() =>
                {
                    try
                    {
                        var docs = doSearchFunc(searcher);

                        foreach (var doc in docs.ScoreDocs)
                        {
                            obs.OnNext(searcher.Doc(doc.Doc));
                            token.ThrowIfCancellationRequested();
                        }

                        obs.OnCompleted();
                    }
                    catch (Exception e)
                    {
                        obs.OnError(e);
                    }
                }, token);

                var disposable = new CompositeDisposable(searcher, reader, cts);
                return disposable;
            });
        }

        public Task DeleteAll()
        {
            return Task.Run(() =>
            {
                SafeWrite(() => _writer.Value.DeleteAll());
                SafeWrite(() => _writer.Value.Commit());
            });
        }

        private void SafeWrite(Action action)
        {
            try
            {
                action();
            }
            catch (OutOfMemoryException)
            {
                // rebuild the writer when out of memory
                RebuildWriter();
                throw;
            }
        }

        private void RebuildWriter()
        {
            _writer = new Lazy<IndexWriter>(() =>
            {
                var writer = new IndexWriter(_directory, _analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
                writer.SetRAMBufferSizeMB(_RAMSizeMb);
                return writer;
            });
        }

        private IndexReader _currentReader;
        private object _readerLock = new object();
        private DateTime _nextRead = DateTime.MinValue;

        private IndexReader GetThrottledReader()
        {
            lock(_readerLock)
            {
                if(_isDisposed)
                {
                    throw new ObjectDisposedException("Indexer");
                }

                if(_currentReader == null || _nextRead < DateTime.UtcNow)
                {
                    if(_currentReader != null) { _currentReader.Dispose(); }

                    _currentReader = _writer.Value.GetReader();
                    _nextRead = DateTime.UtcNow.AddMilliseconds(_readThrottleTimeMs);
                }
                
                return _currentReader.Clone(true);
            }
        }

        public void Dispose()
        {
            if(!_isDisposed)
            {
                lock (_readerLock)
                {
                    _isDisposed = true;
                    if (_currentReader != null)
                    {
                        _currentReader.Dispose();
                        _currentReader = null;
                    }
                }

                if(_writer.IsValueCreated)
                {
                    _writer.Value.Dispose();
                }

                _analyzer.Dispose();
                _directory.Dispose();
            }
        }
    }
}
