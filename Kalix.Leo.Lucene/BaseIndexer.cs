using Kalix.Leo.Indexing;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    public abstract class BaseIndexer : IIndexer
    {
        private readonly ILuceneIndexer _indexer;

        protected BaseIndexer(ILuceneIndexer indexer)
        {
            _indexer = indexer;
        }

        public Task Index(StoreDataDetails details)
        {
            return Index(_indexer, details);
        }

        protected abstract Task Index(ILuceneIndexer indexer, StoreDataDetails details);
    }
}
