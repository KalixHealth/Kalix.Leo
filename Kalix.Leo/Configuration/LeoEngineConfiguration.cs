using Kalix.Leo.Queue;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;

namespace Kalix.Leo.Configuration
{
    public class LeoEngineConfiguration
    {
        public IOptimisticStore BaseStore { get; set; }
        public ISecureStore SecureStore { get; set; }

        public string UniqueIdGeneratorPath { get; set; }

        public IQueue BackupQueue { get; set; }
        public IStore BackupStore { get; set; }

        public IQueue IndexQueue { get; set; }
        public ISecureStore IndexStore { get; set; }
        public Func<string, Type> TypeResolver { get; set; }

        public Action<Exception> UncaughtExceptions { get; set; }

        public IEnumerable<ItemConfiguration> Objects { get; set; }
    }
}
