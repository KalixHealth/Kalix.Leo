namespace Kalix.Leo
{
    /// <summary>
    /// Index of a specific store record
    /// </summary>
    public struct StoreLocation
    {
        private readonly string _container;
        private readonly string _basePath;
        private readonly long? _id;

        public StoreLocation(string container, string basePath)
        {
            _container = container;
            _basePath = basePath;
            _id = null;
        }

        public StoreLocation(string container, string basePath, long? id)
        {
            _container = container;
            _basePath = basePath;
            _id = id;
        }

        /// <summary>
        /// High level container, you can delete containers simply in Azure so consider this
        /// </summary>
        public string Container { get { return _container; } }

        /// <summary>
        /// Can be a folder path, or if your id is null it could be a file name
        /// (assuming storeOptions does not specify to generate an id)
        /// </summary>
        public string BasePath { get { return _basePath; } }

        /// <summary>
        /// Identifier for this specific container/basepath (and ultimately secure store)
        /// </summary>
        public long? Id { get { return _id; } }
    }
}
