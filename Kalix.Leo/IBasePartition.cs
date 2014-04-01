using Kalix.Leo.Indexing;
using Kalix.Leo.Lucene;

namespace Kalix.Leo
{
    /// <summary>
    /// Base implementation of partitions
    /// </summary>
    public interface IBasePartition
    {
        /// <summary>
        /// Index using table storage in this particular partition
        /// </summary>
        /// <typeparam name="TMain">Pulled from the composition type</typeparam>
        /// <typeparam name="TSearch">Pulled from the composition type</typeparam>
        /// <param name="composition">The pre-configured composition object to search with</param>
        /// <returns>A search index to search using the specified composition</returns>
        ISearchIndex<TMain, TSearch> Index<TMain, TSearch>(IRecordSearchComposition<TMain, TSearch> composition);

        /// <summary>
        /// Finds the full text index for this particular partition
        /// Should only use this in the 'Index' phase
        /// </summary>
        ILuceneIndex LuceneIndex { get; }
    }
}
