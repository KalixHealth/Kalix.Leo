using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Streams
{
    public sealed class ReadStreamWrapper : IReadAsyncStream
    {
        private readonly Stream _stream;

        public ReadStreamWrapper(Stream stream)
        {
            if(!stream.CanRead) { throw new ArgumentException("Stream is not a read stream", "stream"); }

            _stream = stream;
        }

        public Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            return _stream.ReadAsync(buffer, offset, count, ct);
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
