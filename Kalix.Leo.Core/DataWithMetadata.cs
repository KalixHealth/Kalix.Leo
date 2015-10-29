namespace Kalix.Leo
{
    /// <summary>
    /// Data stream that also holds metadata
    /// </summary>
    public sealed class DataWithMetadata
    {
        private readonly Metadata _metadata;
        private readonly IReadAsyncStream _stream;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="stream">A stream of data</param>
        /// <param name="metadata">Metadata to include</param>
        public DataWithMetadata(IReadAsyncStream stream, Metadata metadata = null)
        {
            _metadata = metadata ?? new Metadata();
            _stream = stream;
        }

        /// <summary>
        /// Metadata for the associated stream of data
        /// </summary>
        public Metadata Metadata { get { return _metadata; } }

        /// <summary>
        /// The stream of full data
        /// </summary>
        public IReadAsyncStream Stream { get { return _stream; } }
    }
}
