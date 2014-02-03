using System;

namespace Kalix.Leo.Storage
{
    /// <summary>
    /// Pipeline options for a record that will be securely stored
    /// </summary>
    [Flags]
    public enum SecureStoreOptions
    {
        None = 0,
        /// <summary>
        /// Create a message to eventually back this record up to another store
        /// </summary>
        Backup = 1,
        /// <summary>
        /// Create a message to eventually index this record
        /// </summary>
        Index = 2,
        /// <summary>
        /// Compress this record when saving
        /// </summary>
        Compress = 5,
        /// <summary>
        /// When deleting records do not actually delete the record
        /// </summary>
        KeepDeletes = 8,

        All = Backup | Index | Compress | KeepDeletes
    }
}
