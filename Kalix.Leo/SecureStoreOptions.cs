using System;

namespace Kalix.Leo
{
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
