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
        /// Create a message to eventually back this record up to another store
        /// </summary>
        Backup = 1,
        /// <summary>
        /// Create a message to eventually index this record
        /// </summary>
        Index = 2,
        /// <summary>
        /// Encrypt this record when saving
        /// </summary>
        Encrypt = 4,
        /// <summary>
        /// Compress this record when saving
        /// </summary>
        Compress = 8,
        /// <summary>
        /// Generate an id if the record has no id
        /// </summary>
        GenerateId = 16,
        /// <summary>
        /// When deleting records do not actually delete the record
        /// </summary>
        KeepDeletes = 32,

        All = Backup | Index | Encrypt | Compress | GenerateId | KeepDeletes
    }
}
