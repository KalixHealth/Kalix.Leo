using Kalix.Leo.Indexing;
using Kalix.Leo.Lucene;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Internal
{
    public class ItemPartitionIndexer : IIndexer
    {
        private readonly ILeoIndexer _indexer;
        private readonly Func<string, ILuceneIndex> _index;

        public ItemPartitionIndexer(ILeoIndexer indexer, Func<string, ILuceneIndex> index)
        {
            _indexer = indexer;
            _index = index;
        }

        public Task Index(StoreDataDetails details)
        {
            return _indexer.Index(details, _index(details.Container));
        }
    }
}
