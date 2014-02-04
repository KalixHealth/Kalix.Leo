namespace Kalix.Leo
{
    public struct IdWithMetadata
    {
        private readonly long _id;
        private readonly Metadata _metadata;

        public IdWithMetadata(long id, Metadata metadata)
        {
            _id = id;
            _metadata = metadata;
        }

        public long Id { get { return _id; } }
        public Metadata Metadata { get { return _metadata; } }
    }
}
