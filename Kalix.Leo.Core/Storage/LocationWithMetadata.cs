namespace Kalix.Leo.Storage
{
    public sealed class LocationWithMetadata
    {
        private readonly Metadata _metadata;
        private readonly StoreLocation _location;

        public LocationWithMetadata(StoreLocation location, Metadata metadata = null)
        {
            _metadata = metadata ?? new Metadata();
            _location = location;
        }

        public Metadata Metadata { get { return _metadata; } }
        public StoreLocation Location { get { return _location; } }
    }
}
