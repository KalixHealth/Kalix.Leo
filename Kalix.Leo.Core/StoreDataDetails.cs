using System.Collections.Generic;

namespace Kalix.Leo
{
    public class StoreDataDetails
    {
        public string Container { get; set; }
        public string BasePath { get; set; }
        public long? Id { get; set; }

        public IDictionary<string, string> Metadata { get; set; }

        public StoreLocation GetLocation()
        {
            return new StoreLocation(Container, BasePath, Id);
        }
    }
}
