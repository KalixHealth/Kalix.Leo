using System;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    /// <summary>
    /// Document partition is closer to the underlying storage mechanism than the object partition
    /// All records are still partitioned by an id, but individual files are accessed by a string
    /// </summary>
    public interface IDocumentPartition : IBasePartition
    {
        /// <summary>
        /// Save some data into the partition at the specified path
        /// </summary>
        /// <param name="path">The location to save the record (in this particular partition)</param>
        /// <param name="data">The stream of data to save</param>
        /// <param name="metadata">Optional metadata to save - note this is NOT encrypted</param>
        /// <returns>Task that completes when the record is saved</returns>
        Task Save(string path, IObservable<byte[]> data, Metadata metadata = null);

        /// <summary>
        /// Read the data from a specified path in this partition
        /// </summary>
        /// <param name="path">The location to read this record from</param>
        /// <param name="snapshot">Optional paramater to find an old record</param>
        /// <returns>Data stream with any additional metadata</returns>
        Task<DataWithMetadata> Load(string path, string snapshot = null);

        /// <summary>
        /// Gets just the metadata for a file at the specified path in the partition
        /// </summary>
        /// <param name="path">The location to read this record from</param>
        /// <param name="snapshot">Optional paramater to find an old record</param>
        /// <returns>Metadata of the file</returns>
        Task<Metadata> GetMetadata(string path, string snapshot = null);

        /// <summary>
        /// Finds all the snapshots (and associated metadata) of a path
        /// </summary>
        /// <param name="path">The location to find snapshots</param>
        /// <returns>A list of snapshots</returns>
        IObservable<Snapshot> FindSnapshots(string path);

        /// <summary>
        /// Find all files in this partition that start with a prefix
        /// </summary>
        /// <param name="prefix">The prefix to search for, if none provided will find all files in this partition</param>
        /// <returns>A list of paths and metadata</returns>
        IObservable<PathWithMetadata> Find(string prefix = null);

        /// <summary>
        /// Delete a record at the specified location (will not delete snapshots though)
        /// </summary>
        /// <param name="path">The location of the file to delete</param>
        /// <returns>Task that completes when the delete is successful</returns>
        Task Delete(string path);

        /// <summary>
        /// Delete a record at the specified location, and any snapshots as well
        /// </summary>
        /// <param name="path">The location of the file to delete</param>
        /// <returns>Task that completes when ALL deletes are successful</returns>
        Task DeletePermanent(string path);

        /// <summary>
        /// If this partition is configured to index, will find all files and queue an index message
        /// </summary>
        /// <returns>Task that completes when all files have been queued to index</returns>
        Task ReIndexAll();

        /// <summary>
        /// If this partition is configured to backup, will find all files and queue a backup message
        /// </summary>
        /// <returns>Task that completes when all files have been queued to backup</returns>
        Task ReBackupAll();
    }
}
