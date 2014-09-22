using System;
using System.IO;

namespace Kalix.Leo
{
    /// <summary>
    /// Data stream that also holds metadata
    /// </summary>
    public sealed class DataWithMetadata
    {
        private readonly Metadata _metadata;
        private readonly Stream _stream;
        private bool _isDisposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="stream">A stream of data</param>
        /// <param name="metadata">Metadata to include</param>
        /// <param name="onDispose">Any action to take when this object is disposed</param>
        public DataWithMetadata(Stream stream, Metadata metadata = null)
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
        public Stream Stream { get { return _stream; } }
    }
}
