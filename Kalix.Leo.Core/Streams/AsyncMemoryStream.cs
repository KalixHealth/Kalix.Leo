using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Streams
{
    /// <summary>
    /// Memory stream that implements IWriteAsyncStream
    /// </summary>
    public class AsyncMemoryStream : IWriteAsyncStream
    {
        private readonly MemoryStream _stream;
        private bool _isDisposed;

        /// <summary>
        /// Creates an async memory stream with the default constructor
        /// </summary>
        public AsyncMemoryStream()
        {
            _stream = new MemoryStream();
        }

        /// <summary>
        /// Creates an async memory stream with a given capacity
        /// </summary>
        public AsyncMemoryStream(int capacity)
        {
            _stream = new MemoryStream(capacity);
        }

        /// <summary>
        /// Length of the underlying memory stream
        /// </summary>
        public int Length
        {
            get
            {
                if (_isDisposed) { throw new ObjectDisposedException("_stream"); };
                return (int)_stream.Length;
            }
        }

        /// <summary>
        /// Get Buffer of underlying memory stream
        /// Note: This buffer may have longer than the length of the stream
        /// </summary>
        public byte[] GetBuffer()
        {
            if (_isDisposed) { throw new ObjectDisposedException("_stream"); };
            return _stream.GetBuffer();
        }

        /// <summary>
        /// Calls ToArray of the underlying memory stream and returns the result
        /// </summary>
        public byte[] ToArray()
        {
            if (_isDisposed) { throw new ObjectDisposedException("_stream"); };
            return _stream.ToArray();
        }

        /// <summary>
        /// Write to the underlying memory stream
        /// </summary>
        public Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            if (_isDisposed) { throw new ObjectDisposedException("_stream"); };
            _stream.Write(buffer, offset, count);
            ct.ThrowIfCancellationRequested();
            return Task.FromResult(0);
        }

        /// <summary>
        /// Complete does nothing for a memory stream
        /// </summary>
        public Task Complete(CancellationToken ct)
        {
            return Task.FromResult(0);
        }

        /// <summary>
        /// In this case we should dispose the memory stream
        /// </summary>
        /// <returns></returns>
        public Task Cancel()
        {
            Dispose();
            return Task.FromResult(0);
        }

        /// <summary>
        /// Flush does nothing for a memory stream
        /// </summary>
        public Task FlushAsync(CancellationToken ct)
        {
            if (_isDisposed) { throw new ObjectDisposedException("_stream"); };
            return Task.FromResult(0);
        }
        
        /// <summary>
        /// Disposes the underlying memory stream
        /// </summary>
        public void Dispose()
        {
            if (!_isDisposed)
            {
                _stream.Dispose();
                _isDisposed = true;
            }
        }
    }
}
