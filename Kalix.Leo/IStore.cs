using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface IStore
    {
        /// <summary>
        /// Save Data to a specified location
        /// </summary>
        /// <param name="data">Read stream of data</param>
        /// <param name="location">Location to store the file</param>
        Task SaveData(Stream data, StoreLocation location);

        /// <summary>
        /// Gets the file data. If the file is 'soft' deleted will return null.
        /// </summary>
        /// <param name="location">Location of the stored file</param>
        /// <returns>A read stream of data, or null if not found</returns>
        Task<Stream> LoadData(StoreLocation location); 

        /// <summary>
        /// Takes a snapshot of a file location
        /// </summary>
        /// <param name="location">Location of the file</param>
        /// <returns>Time/Identifier of the snapshot</returns>
        Task<DateTime> TakeSnapshot(StoreLocation location);

        /// <summary>
        /// Find a list of snapshots. Ignores the fact if the file is 'soft' deleted. 
        /// </summary>
        /// <param name="earilest">The time to find snapshots from</param>
        /// <param name="latest">The time to stop finding snapshots</param>
        /// <param name="take">The maximum number of snapshots to find</param>
        /// <returns>List of snapshot dates</returns>
        Task<IEnumerable<DateTime>> FindAllSnapshots(DateTime? earilest = null, DateTime? latest = null, int? take = 10);

        /// <summary>
        /// Gets the data of a snapshot
        /// </summary>
        /// <param name="location">Location of the file</param>
        /// <param name="snapshot">The snapshot identifier</param>
        /// <returns>A read stream of the data, or null if not found</returns>
        Task<Stream> LoadSnapshotData(StoreLocation location, DateTime snapshot);

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

        Task CreateContainerIfNotExists(string container);
        Task PermanentDeleteContainer(string container);
    }
}
