using System;
using System.Collections.Generic;

namespace Kalix.Leo
{
    public class Snapshot
    {
        public DateTime Id { get; set; }

        public IDictionary<string, string> Metadata { get; set; }
    }
}
