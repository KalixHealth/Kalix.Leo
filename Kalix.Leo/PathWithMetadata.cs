namespace Kalix.Leo
{
    public struct PathWithMetadata
    {
        private readonly string _path;
        private readonly Metadata _metadata;

        public PathWithMetadata(string path, Metadata metadata)
        {
            _path = path;
            _metadata = metadata;
        }

        public string Path { get { return _path; } }
        public Metadata Metadata { get { return _metadata; } }
    }
}
