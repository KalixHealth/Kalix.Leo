using Kalix.ApiCrypto.RSA;
using Kalix.Leo.Compression;
using Kalix.Leo.Queue;
using Kalix.Leo.Storage;
using Kalix.Leo.Table;
using System;
using System.Collections.Generic;

namespace Kalix.Leo.Configuration
{
    public class LeoEngineConfiguration
    {
        public string UniqueName { get; set; }
        public IOptimisticStore BaseStore { get; set; }
        public ITableClient TableStore { get; set; }

        public string UniqueIdGeneratorPath { get; set; }

        public string KeyContainer { get; set; }
        public RSAServiceProvider RsaCert { get; set; }

        public IQueue BackupQueue { get; set; }
        public IStore BackupStore { get; set; }

        public IQueue IndexQueue { get; set; }
        public IOptimisticStore IndexStore { get; set; } 
        public Func<string, Type> TypeNameResolver { get; set; }
        
        public Action<Exception> UncaughtExceptions { get; set; }

        public ICompressor Compressor { get; set; }

        public IEnumerable<ItemConfiguration> Objects { get; set; }
        public Func<Type, object> TypeResolver { get; set; }
    }
}
