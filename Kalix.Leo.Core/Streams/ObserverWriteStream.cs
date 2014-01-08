using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Streams
{
    public class ObserverWriteStream : Stream
    {
        private readonly IObserver<byte> _observer;
        private readonly Lazy<bool> _firstHit;

        public ObserverWriteStream(IObserver<byte> observer, Action firstHit = null)
        {
            _observer = observer;
            _firstHit = new Lazy<bool>(() =>
            {
                if (firstHit != null) { firstHit(); }
                return true;
            });
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
            // Do nothing
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var dummy = _firstHit.Value;
            for (int i = 0; i < count; i++)
            {
                _observer.OnNext(buffer[offset + i]);
            }
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Write(buffer, offset, count);
            return Task.FromResult(0);
        }

        public override long Length
        {
            get { throw new NotImplementedException(); }
        }

        public override long Position
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }
    }
}
