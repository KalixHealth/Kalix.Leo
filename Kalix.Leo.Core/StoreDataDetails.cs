using Kalix.Leo.Storage;

namespace Kalix.Leo;

public class StoreDataDetails
{
    public string Container { get; set; }
    public string BasePath { get; set; }
    public long? Id { get; set; }

    public Metadata Metadata { get; set; }

    public StoreLocation GetLocation()
    {
        return new StoreLocation(Container, BasePath, Id);
    }
}