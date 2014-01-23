namespace Kalix.Leo
{
    public struct IdWithMetadata
    {
        private readonly long _id;
        private readonly IMetadata _metadata;

        public IdWithMetadata(long id, IMetadata metadata)
        {
            _id = id;
            _metadata = metadata;
        }

        public long Id { get { return _id; } }
        public IMetadata Metadata { get { return _metadata; } }
    }
}
