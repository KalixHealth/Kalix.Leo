using System;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Linq;

namespace Kalix.Leo.Streams
{
    public class ObservableReadStream : Stream
    {
        private readonly Lazy<IEnumerator<byte>> _data;

        public ObservableReadStream(IObservable<byte> data)
        {
            // Turn the IObservable into a 'pull' sequence
            _data = new Lazy<IEnumerator<byte>>(() => data.ToEnumerable().GetEnumerator());
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var en = _data.Value;

            int written = 0;
            while(written < count && en.MoveNext())
            {
                buffer[offset] = en.Current;
                written++;
                offset++;
            }

            return written;
        }

        public override void Flush()
        {
            throw new NotImplementedException();
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

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
