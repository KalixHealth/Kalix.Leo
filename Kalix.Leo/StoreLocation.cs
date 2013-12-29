namespace Kalix.Leo
{
    /// <summary>
    /// Index of a specific store record
    /// </summary>
    public class StoreLocation
    {
        /// <summary>
        /// High level container, you can delete containers simply in Azure so consider this
        /// </summary>
        public string Container { get; set; }

        /// <summary>
        /// Can be a folder path, or if your id is null it could be a file name
        /// (assuming storeOptions does not specify to generate an id)
        /// </summary>
        public string BasePath { get; set; }

        /// <summary>
        /// Identifier for this specific container/basepath (and ultimately secure store)
        /// </summary>
        public long? Id { get; set; }
    }
}
