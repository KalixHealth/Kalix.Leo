using Kalix.Leo.Storage;
using System;

namespace Kalix.Leo
{
    public class Snapshot
    {
        public string Id { get; set; }

        public IMetadata Metadata { get; set; }
    }
}
