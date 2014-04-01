using Kalix.Leo.Indexing;
using System;

namespace Kalix.Leo.Configuration
{
    /// <summary>
    /// ItemConfiguration helps to define a partition and how it is used
    /// </summary>
    public class ItemConfiguration
    {
        /// <summary>
        /// Only define type for an Object Partition type, otherwise null
        /// </summary>
        public Type Type { get; set; }

        /// <summary>
        /// The basepath to help namespace this partition, used when records are saved to storage
        /// </summary>
        public string BasePath { get; set; }

        /// <summary>
        /// Optional - the type of the indexer to use when a record is saved. Must be of type IIndexer
        /// </summary>
        public Type Indexer { get; set; }

        /// <summary>
        /// Whether to encrypt records in this partition
        /// </summary>
        public bool DoEncrypt { get; set; }

        /// <summary>
        /// Whether to backup files saved on this partition (only applies to base object, not any table storage)
        /// </summary>
        public bool DoBackup { get; set; }

        /// <summary>
        /// Whether to compress objects (Note this stage happens before encryption)
        /// </summary>
        public bool DoCompress { get; set; }
    }
}
