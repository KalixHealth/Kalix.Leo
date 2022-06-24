using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Storage;

/// <summary>
/// A store that has locking capabilities, required for the secure store...
/// </summary>
public interface IOptimisticStore : IStore
{
    /// <summary>
    /// Save data to a specified location, but put a lock on it while writing. Does not support multipart...
    /// </summary>
    /// <param name="metadata">Metadata to save, must include an eTag</param>
    /// <param name="audit">Audit information to save, note that the created by/created on fields will be ignored</param>
    /// <param name="savingFunc">A write stream so you can do what you want to save</param>
    /// <param name="location">Location to store the file</param>
    /// <param name="token">Cancellation token</param>
    /// <returns>Whether the write was successful or not</returns>
    Task<OptimisticStoreWriteResult> TryOptimisticWrite(StoreLocation location, Metadata metadata, UpdateAuditInfo audit, Func<PipeWriter, Task<long?>> savingFunc, CancellationToken token);

    /// <summary>
    /// Locks the storage at the specified location
    /// </summary>
    /// <param name="location">Location of the file to lock</param>
    /// <returns>A disposable to release the lock, or null if the lock could not be made</returns>
    Task<(IAsyncDisposable CancelDispose, Task Task)> Lock(StoreLocation location);

    /// <summary>
    /// Lock based timer that allows a task to be synchronised to be run every x time over all shared instances
    /// </summary>
    /// <param name="location">Location of the lock that will manage synchronisation</param>
    /// <param name="interval">The interval that you would like a task to fire (over all instances)</param>
    /// <param name="unhandledExceptions">Any unexpected errors during the lock loop can be handled here (optional)</param>
    /// <returns>An observable that fires once over the timespan over all instances</returns>
    IAsyncEnumerable<bool> RunEvery(StoreLocation location, TimeSpan interval, Action<Exception> unhandledExceptions = null, CancellationToken token = default);

    /// <summary>
    /// Lock based method that runs a task only once (ever). 
    /// </summary>
    /// <param name="location">The location of the lock, the action will only run if this location has not been used before</param>
    /// <param name="action">The action to execute once</param>
    /// <returns>Task for when the action is complete</returns>
    Task RunOnce(StoreLocation location, Func<Task> action);
}

/// <summary>
/// Save result object
/// </summary>
public class OptimisticStoreWriteResult
{
    /// <summary>
    /// Whether the save was successful or not
    /// </summary>
    public bool Result { get; set; }

    /// <summary>
    /// Associated metadata
    /// </summary>
    public Metadata Metadata { get; set; }
}