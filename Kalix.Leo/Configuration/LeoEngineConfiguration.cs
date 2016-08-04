using Kalix.ApiCrypto.RSA;
using Kalix.Leo.Compression;
using Kalix.Leo.Queue;
using Kalix.Leo.Storage;
using Kalix.Leo.Table;
using System;
using System.Collections.Generic;

namespace Kalix.Leo.Configuration
{
    /// <summary>
    /// The configuration for the Leo Engine
    /// </summary>
    public class LeoEngineConfiguration
    {
        /// <summary>
        /// Used when caching partitions, each instance of the leo engine running side by side should have a different unique name
        /// </summary>
        public string UniqueName { get; set; }

        /// <summary>
        /// The main underlying data store
        /// </summary>
        public IOptimisticStore BaseStore { get; set; }

        /// <summary>
        /// The main underlying table data store
        /// </summary>
        public ITableClient TableStore { get; set; }

        /// <summary>
        /// The path in each partition/basepath that is used to store the id generator for a particular partition
        /// </summary>
        public string UniqueIdGeneratorPath { get; set; }

        /// <summary>
        /// A container name that will hold all the encryption keys used for each partition
        /// </summary>
        public string KeyContainer { get; set; }

        /// <summary>
        /// The RSA certificate that can decode the key container (needs to contain a private key)
        /// </summary>
        public RSAServiceProvider RsaCert { get; set; }

        /// <summary>
        /// The queue that will be used to send backup messages
        /// </summary>
        public IQueue BackupQueue { get; set; }

        /// <summary>
        /// The store that will be used to backup files
        /// </summary>
        public IStore BackupStore { get; set; }

        /// <summary>
        /// The queue that will be used to send index messages
        /// </summary>
        public IQueue IndexQueue { get; set; }

        /// <summary>
        /// The store that will hold the index (Specifically the full text lucene index)
        /// </summary>
        public IOptimisticStore IndexStore { get; set; }

        /// <summary>
        /// If set, then the underlying Lucene engine will use a MMapDirectoy instead of a RamDirectory as a cache in the given folder
        /// </summary>
        public string LuceneCacheBasePath { get; set; }

        /// <summary>
        /// An optional property to be able to hook into any uncaught exceptions in the leo engine
        /// Often called from messages that fail etc
        /// </summary>
        public Action<Exception> UncaughtExceptions { get; set; }

        /// <summary>
        /// The compressor to use for any files that need compressing
        /// </summary>
        public ICompressor Compressor { get; set; }

        /// <summary>
        /// The configurations of the objects that will be used in the engine. If an object is not specified here there will be error thrown
        /// when trying to create a partition
        /// </summary>
        public IEnumerable<ItemConfiguration> Objects { get; set; }

        /// <summary>
        /// Type resolver to find an object by type, should use a container if you have one
        /// </summary>
        public Func<Type, object> TypeResolver { get; set; }

        /// <summary>
        /// The resolvers to find a type by name (if null will use a default implementation)
        /// </summary>
        public Func<string, Type> TypeNameResolver { get; set; }
    }
}
