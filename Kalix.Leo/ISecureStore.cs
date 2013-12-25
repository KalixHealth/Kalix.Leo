using System;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface ISecureStore
    {
        /// <summary>
        /// Save a stream of data to a storage location, might start a cascade of backups/updates
        /// </summary>
        /// <param name="data">Read stream of data to save</param>
        /// <param name="location">Location to save the data</param>
        /// <param name="storeOptions">Options as to how to save the data - KeepDeletes option is not used</param>
        /// <returns>The final location</returns>
        Task<StoreLocation> SaveRecord(Stream data, StoreLocation location, SecureStoreOptions storeOptions = SecureStoreOptions.All);

        /// <summary>
        /// Delete a record from the secret store
        /// </summary>
        /// <param name="location">Location of the data to delete</param>
        /// <param name="storeOptions">Options of how to delete the data - Encrypt/Compress/GenerateId options are ignored</param>
        Task DeleteRecord(StoreLocation location, SecureStoreOptions storeOptions = SecureStoreOptions.All);
    }

    /// <summary>
    /// Index of a specific store record
    /// </summary>
    public class StoreLocation
    {
        /// <summary>
        /// High level container, you can delete containers simply in Azure so consider this
        /// </summary>
        public string Container { get; set; }

        /// <summary>
        /// Can be a folder path, or if your id is null it could be a file name
        /// (assuming storeOptions does not specify to generate an id)
        /// </summary>
        public string BasePath { get; set; }

        /// <summary>
        /// Identifier for this specific container/basepath (and ultimately secure store)
        /// </summary>
        public long? Id { get; set; }
    }

    /// <summary>
    /// Pipeline options for a record that will be securely stored
    /// </summary>
    [Flags]
    public enum SecureStoreOptions
    {
        /// <summary>
        /// Create a snapshot of the old record before saving the new one
        /// </summary>
        Snapshot = 1,
        /// <summary>
        /// Create a message to eventually back this record up to another store
        /// </summary>
        Backup = 2,
        /// <summary>
        /// Create a message to eventually index this record
        /// </summary>
        Index = 4,
        /// <summary>
        /// Encrypt this record when saving
        /// </summary>
        Encrypt = 8,
        /// <summary>
        /// Compress this record when saving
        /// </summary>
        Compress = 16,
        /// <summary>
        /// Generate an id if the record has no id
        /// </summary>
        GenerateId = 32,
        /// <summary>
        /// When deleting records do not actually delete the record
        /// </summary>
        KeepDeletes = 64,

        All = Snapshot | Backup | Index | Encrypt | Compress | GenerateId | KeepDeletes
    }
}
