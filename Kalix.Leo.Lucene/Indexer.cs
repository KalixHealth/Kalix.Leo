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
    public class Indexer : IDisposable
    {
        private readonly Lazy<IndexWriter> _writer;
        private readonly IFileCache _cache;
        private readonly Directory _directory;
        private readonly Analyzer _analyzer;
        private readonly int _readThrottleTimeMs;

        private bool _isDisposed;

        /// <summary>
        /// Create a lucene index over the top of a secure store, using an encrypted file cache and english analyzer
        /// Only one instance should be used for both indexing and searching (on any number of threads) for best results
        /// </summary>
        /// <param name="store">Store to have the Indexer on top of</param>
        /// <param name="container">Container to put the index</param>
        /// <param name="readIndexThottleMs">The time before the read index can be refreshed, defaults to five seconds</param>
        /// <param name="RAMSize">The max amount of memory to use before flushing when writing</param>
        public Indexer(ISecureStore store, string container, int readIndexThottleMs = 5000, int RAMSize = 20971520)
        {
            _cache = new EncryptedFileCache();
            _directory = new SecureStoreDirectory(store, container, _cache);
            _analyzer = new EnglishAnalyzer();
            _readThrottleTimeMs = readIndexThottleMs;

            _writer = new Lazy<IndexWriter>(() => new IndexWriter(_directory, _analyzer, IndexWriter.MaxFieldLength.UNLIMITED));
        }

        public async Task WriteToIndex(IObservable<Document> documents)
        {
            await documents
                .Do((d) =>
                {
                    _writer.Value.AddDocument(d);
                },
                () =>
                {
                    _writer.Value.Commit();
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
                _writer.Value.DeleteAll();
                _writer.Value.Commit();
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
                _cache.Dispose();
            }
        }
    }
}
