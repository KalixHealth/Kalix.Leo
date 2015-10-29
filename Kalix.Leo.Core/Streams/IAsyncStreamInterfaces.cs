using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    /// <summary>
    /// Base interface for reading from a SLOW stream
    /// </summary>
    public interface IReadAsyncStream : IDisposable
    {
        /// <summary>
        /// Asynchronously reads a sequence of bytes from the current stream and advances
        /// the position within the stream by the number of bytes read.
        /// </summary>
        /// <param name="buffer">
        /// An array of bytes. When this method returns, the buffer contains the specified
        /// byte array with the values between offset and (offset + count - 1) replaced by
        /// the bytes read from the current source.
        /// </param>
        /// <param name="offset">
        /// The zero-based byte offset in buffer at which to begin storing the data read
        /// from the current stream.
        /// </param>
        /// <param name="count">
        /// The maximum number of bytes to be read from the current stream.
        /// </param>
        /// <param name="ct">
        /// The token to monitor for cancellation requests.
        /// </param>
        /// <returns>
        /// A task that represents the asynchronous read operation. The value of the TResult
        /// parameter contains the total number of bytes read into the buffer. The result
        /// value can be less than the number of bytes requested if the number of bytes currently
        /// available is less than the requested number, or it can be 0 (zero) if the end
        /// of the stream has been reached.
        /// </returns>
        Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct);
    }

    /// <summary>
    /// Base interface for writing to a SLOW stream
    /// NOTE: Dispose only is there to free up resources, will not call to underlying stream
    /// </summary>
    public interface IWriteAsyncStream : IDisposable
    {
        /// <summary>
        /// Asynchronously writes a sequence of bytes to the current stream, advances the
        /// current position within this stream by the number of bytes written, and monitors
        /// cancellation requests.
        /// </summary>
        /// <param name="buffer">
        /// The buffer to write data from.
        /// </param>
        /// <param name="offset">
        /// The zero-based byte offset in buffer from which to begin copying bytes to the
        /// stream.
        /// </param>
        /// <param name="count">
        /// The maximum number of bytes to write.
        /// </param>
        /// <param name="ct">
        /// The token to monitor for cancellation requests.
        /// </param>
        /// <returns>
        /// A task that represents the asynchronous write operation.
        /// </returns>
        Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct);

        /// <summary>
        /// Asynchronously clears all buffers for this stream and causes any buffered data
        /// to be written to the underlying device.
        /// </summary>
        /// <param name="ct">
        /// The token to monitor for cancellation requests.
        /// </param>
        /// <returns>
        /// A task that represents the asynchronous flush operation.
        /// </returns>
        Task FlushAsync(CancellationToken ct);

        /// <summary>
        /// Many write operations need to indicate that writing is complete to flush any last blocks
        /// </summary>
        /// <param name="ct">
        /// The token to monitor for cancellation requests.
        /// </param>
        /// <returns>
        /// A task that represents the asynchronous complete operation.
        /// </returns>
        Task Complete(CancellationToken ct);
    }
}
