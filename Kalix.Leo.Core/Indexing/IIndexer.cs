using System.Threading.Tasks;

namespace Kalix.Leo.Indexing
{
    /// <summary>
    /// An interface that is called when a record has been updated
    /// </summary>
    public interface IIndexer
    {
        /// <summary>
        /// This implementation gets called when a record is updated, only contains the location and metadata of the record
        /// </summary>
        /// <param name="details">The location and metadata of the record that should be indexed</param>
        /// <returns>A Task for when the index is complete and successful</returns>
        Task Index(StoreDataDetails details);
    }

    /// <summary>
    /// Generic version of the indexer
    /// </summary>
    /// <typeparam name="T">The object type that is being indexed</typeparam>
    public interface IIndexer<T> : IIndexer { }
}
