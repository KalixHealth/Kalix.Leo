namespace Kalix.Leo
{
    public struct PathWithMetadata
    {
        private readonly string _path;
        private readonly IMetadata _metadata;

        public PathWithMetadata(string path, IMetadata metadata)
        {
            _path = path;
            _metadata = metadata;
        }

        public string Path { get { return _path; } }
        public IMetadata Metadata { get { return _metadata; } }
    }
}
