using Kalix.Leo.Compression;
using Kalix.Leo.Storage;
using Kalix.Leo.Table;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;

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
        public RSA RsaCert { get; set; }

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
    }
}
