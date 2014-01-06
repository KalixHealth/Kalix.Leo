using System;
using System.Collections.Generic;

namespace Kalix.Leo
{
    public class Snapshot
    {
        public string Id { get; set; }
        public DateTime Modified { get; set; }

        public IDictionary<string, string> Metadata { get; set; }
    }
}
