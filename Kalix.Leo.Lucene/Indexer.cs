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
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    public class Indexer : IDisposable
    {
        private readonly Lazy<IndexWriter> _writer;
        private readonly IFileCache _cache;
        private readonly Directory _directory;
        private readonly Analyzer _analyzer;

        private bool _isDisposed;

        public Indexer(ISecureStore store, string container)
        {
            _cache = new EncryptedFileCache();
            _directory = new SecureStoreDirectory(store, container, _cache);
            _analyzer = new EnglishAnalyzer();

            _writer = new Lazy<IndexWriter>(() => new IndexWriter(_directory, _analyzer, IndexWriter.MaxFieldLength.UNLIMITED));
        }

        public Task WriteToIndex(Func<IndexWriter, Analyzer, Task> doWritesFunc)
        {
            return Task.Run(async () =>
            {
                await doWritesFunc(_writer.Value, _analyzer).ConfigureAwait(false);
                _writer.Value.Commit();
            });          
        }

        public IObservable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc)
        {
            return Observable.Create<Document>((obs, ct) =>
            {
                return Task.Run(() =>
                {
                    var reader = _writer.Value.GetReader();
                    var searcher = new IndexSearcher(reader);

                    try
                    {
                        var docs = doSearchFunc(searcher);

                        foreach (var doc in docs.ScoreDocs)
                        {
                            obs.OnNext(searcher.Doc(doc.Doc));
                        }

                        obs.OnCompleted();
                    }
                    catch (Exception e)
                    {
                        obs.OnError(e);
                    }

                    return new CompositeDisposable(reader, searcher);
                }, ct);
            });
        }

        public void Dispose()
        {
            if(!_isDisposed)
            {
                if(_writer.IsValueCreated)
                {
                    _writer.Value.Dispose();
                }

                _analyzer.Dispose();
                _directory.Dispose();
                _cache.Dispose();
                _isDisposed = true;
            }
        }
    }
}
