using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Storage
{
    /// <summary>
    /// A store that has locking capabilities, required for the secure store...
    /// </summary>
    public interface IOptimisticStore : IStore
    {
        /// <summary>
        /// Save data to a specified location, but put a lock on it while writing. Does not support multipart...
        /// </summary>
        /// <param name="metadata">Metadata to save, must include an eTag</param>
        /// <param name="savingFunc">A write stream so you can do what you want to save</param>
        /// <param name="location">Location to store the file</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Whether the write was successful or not</returns>
        Task<OptimisticStoreWriteResult> TryOptimisticWrite(StoreLocation location, Metadata metadata, Func<IWriteAsyncStream, Task<long?>> savingFunc, CancellationToken token);

        /// <summary>
        /// Locks the storage at the specified location
        /// </summary>
        /// <param name="location">Location of the file to lock</param>
        /// <returns>A disposable to release the lock, or null if the lock could not be made</returns>
        Task<IDisposable> Lock(StoreLocation location);

        /// <summary>
        /// Lock based timer that allows a task to be synchronised to be run every x time over all shared instances
        /// </summary>
        /// <param name="location">Location of the lock that will manage synchronisation</param>
        /// <param name="interval">The interval that you would like a task to fire (over all instances)</param>
        /// <param name="unhandledExceptions">Any unexpected errors during the lock loop can be handled here (optional)</param>
        /// <returns>An observable that fires once over the timespan over all instances</returns>
        IObservable<bool> RunEvery(StoreLocation location, TimeSpan interval, Action<Exception> unhandledExceptions = null);

        /// <summary>
        /// Lock based method that runs a task only once (ever). 
        /// </summary>
        /// <param name="location">The location of the lock, the action will only run if this location has not been used before</param>
        /// <param name="action">The action to execute once</param>
        /// <returns>Task for when the action is complete</returns>
        Task RunOnce(StoreLocation location, Func<Task> action);
    }

    public class OptimisticStoreWriteResult
    {
        public bool Result { get; set; }
        public Metadata Metadata { get; set; }
    }
}
