using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Streams
{
    /// <summary>
    /// Simple wrapper around a read stream
    /// </summary>
    public sealed class ReadStreamWrapper : IReadAsyncStream
    {
        private readonly Stream _stream;

        /// <summary>
        /// Constructor that takes a normal read stream
        /// </summary>
        public ReadStreamWrapper(Stream stream)
        {
            if(stream == null) { throw new ArgumentNullException("stream"); }
            if(!stream.CanRead) { throw new ArgumentException("Stream is not a read stream", "stream"); }

            _stream = stream;
        }

        /// <summary>
        /// Calls the underlying stream's read-async method
        /// </summary>
        public Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            return _stream.ReadAsync(buffer, offset, count, ct);
        }

        /// <summary>
        /// Dispose the underlying stream
        /// </summary>
        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
