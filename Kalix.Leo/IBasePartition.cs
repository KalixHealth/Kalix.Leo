using Kalix.Leo.Lucene;

namespace Kalix.Leo
{
    public interface IBasePartition
    {
        ILuceneIndex Indexer { get; }
    }
}
