using Kalix.Leo.Indexing;
using Kalix.Leo.Lucene;

namespace Kalix.Leo
{
    public interface IBasePartition
    {
        ISearchIndex<TMain, TSearch> Index<TMain, TSearch>(IRecordSearchComposition<TMain, TSearch> composition);

        ILuceneIndex LuceneIndex { get; }
    }
}
