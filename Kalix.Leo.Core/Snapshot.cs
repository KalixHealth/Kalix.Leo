namespace Kalix.Leo
{
    /// <summary>
    /// Simple data structure to hold information about a snapshot
    /// </summary>
    public class Snapshot
    {
        /// <summary>
        /// The id of the snapshot so that it can be accessed
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// The metadata associated with this particular snapshot
        /// </summary>
        public Metadata Metadata { get; set; }
    }
}
