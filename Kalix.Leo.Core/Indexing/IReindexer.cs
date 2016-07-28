using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kalix.Leo.Indexing
{
    /// <summary>
    /// Use this interface instead of IIndexer if you want different behaviour on a reindex
    /// </summary>
    public interface IReindexer : IIndexer
    {
        /// <summary>
        /// This implementation gets called when a record has been manually 'reindexed', so that different behaviour can be called
        /// </summary>
        /// <param name="details">The location and metadata of the record that should be re-indexed</param>
        /// <returns>A Task for when the re-index is complete and successful</returns>
        Task Reindex(IEnumerable<StoreDataDetails> details);
    }

    /// <summary>
    /// Generic version of the reindexer
    /// </summary>
    /// <typeparam name="T">The object type that is being indexed/reindexed</typeparam>
    public interface IReindexer<T> : IReindexer, IIndexer<T> { }
}
