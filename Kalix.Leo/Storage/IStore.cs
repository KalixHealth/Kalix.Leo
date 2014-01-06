using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo.Storage
{
    /// <summary>
    /// An interface to hide an underlying storage mechanism, if you can implement this interface you can use
    /// the Kalix engine
    /// </summary>
    public interface IStore
    {
        /// <summary>
        /// Save Data to a specified location
        /// </summary>
        /// <param name="data">Read stream of data</param>
        /// <param name="location">Location to store the file</param>
        /// <param name="metadata">Any additional user defined metadata</param>
        Task SaveData(Stream data, StoreLocation location, IDictionary<string, string> metadata = null);

        /// <summary>
        /// Save data to a specified location, but put a lock on it while writing
        /// </summary>
        /// <param name="data">Read stream of data</param>
        /// <param name="location">Location to store the file</param>
        /// <param name="metadata">Any additional user defined metadata</param>
        /// <returns>Whether the write was successful or not</returns>
        Task<bool> TryOptimisticWrite(Stream data, StoreLocation location, IDictionary<string, string> metadata = null);

        /// <summary>
        /// Load data from a specified location into a write stream
        /// </summary>
        /// <param name="location">Location of the file to load</param>
        /// <param name="streamPicker">Function to pick a write stream depending on returned metadata</param>
        /// <param name="snapshot">Whether to load a specific snapshot</param>
        /// <returns>Returns whether there was a file found</returns>
        Task<bool> LoadData(StoreLocation location, Func<IDictionary<string, string>, Stream> streamPicker, string snapshot = null); 

        /// <summary>
        /// Find a list of snapshots. Ignores the fact if the file is 'soft' deleted. 
        /// </summary>
        /// <param name="location">The location of the file to find snapshots of</param>
        /// <returns>List of snapshot dates</returns>
        Task<IEnumerable<Snapshot>> FindSnapshots(StoreLocation location);

        /// <summary>
        /// Marks the file as deleted, but snapshots are still available
        /// </summary>
        /// <param name="location">Location of the file</param>
        Task SoftDelete(StoreLocation location);

        /// <summary>
        /// Deletes the file and all snapshots, not recoverable
        /// </summary>
        /// <param name="location">Location of the file</param>
        Task PermanentDelete(StoreLocation location);

        /// <summary>
        /// Make sure a container exists
        /// </summary>
        /// <param name="container">Name of the container to create</param>
        Task CreateContainerIfNotExists(string container);

        /// <summary>
        /// Delete a container if it exists
        /// </summary>
        /// <param name="container">Name of the container to delete</param>
        Task PermanentDeleteContainer(string container);
    }
}
