using System;
using System.Collections.Generic;

namespace Kalix.Leo
{
    public interface IMetadata : IDictionary<string, string>
    {
        DateTime? LastModified { get; set; }
        long? Size { get; set; }
    }
}
