namespace Kalix.Leo.Storage
{
    public sealed class LocationWithMetadata
    {
        private readonly IMetadata _metadata;
        private readonly StoreLocation _location;

        public LocationWithMetadata(StoreLocation location, IMetadata metadata = null)
        {
            _metadata = metadata ?? new Metadata();
            _location = location;
        }

        public IMetadata Metadata { get { return _metadata; } }
        public StoreLocation Location { get { return _location; } }
    }
}
