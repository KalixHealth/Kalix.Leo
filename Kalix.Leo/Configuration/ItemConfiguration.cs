using Kalix.Leo.Indexing;
using System;

namespace Kalix.Leo.Configuration
{
    public class ItemConfiguration
    {
        public Type Type { get; set; }
        public string BasePath { get; set; }

        public ILeoIndexer Indexer { get; set; }
        public bool DoEncrypt { get; set; }
        public bool DoBackup { get; set; }
        public bool DoCompress { get; set; }
    }
}
