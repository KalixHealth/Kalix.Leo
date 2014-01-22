using Kalix.Leo.Indexing;
using Kalix.Leo.Storage;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    public abstract class TypedIndexer<T> : IIndexer<T>
    {
        private readonly ISecureStore _store;
        private readonly ILuceneIndexer _indexer;

        public TypedIndexer(ILuceneIndexer indexer, ISecureStore store)
        {
            _store = store;
            _indexer = indexer;
        }

        public async Task Index(StoreDataDetails details)
        {
            var data = await _store.LoadObject<T>(details.GetLocation());
            await Index(_indexer, data.Data, data.Metadata);
        }

        protected abstract Task Index(ILuceneIndexer indexer, T data, IMetadata metadata);
    }
}
