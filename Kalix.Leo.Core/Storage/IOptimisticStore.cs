using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Storage
{
    public interface IOptimisticStore : IStore
    {
        /// <summary>
        /// Save data to a specified location, but put a lock on it while writing. Does not support multipart...
        /// </summary>
        /// <param name="data">Stream of data and metadata</param>
        /// <param name="location">Location to store the file</param>
        /// <returns>Whether the write was successful or not</returns>
        Task<bool> TryOptimisticWrite(StoreLocation location, DataWithMetadata data);

        /// <summary>
        /// Locks the storage at the specified location
        /// </summary>
        /// <param name="location">Location of the file to lock</param>
        /// <returns>A disposable to release the lock, or null if the lock could not be made</returns>
        Task<IDisposable> Lock(StoreLocation location);
    }
}
