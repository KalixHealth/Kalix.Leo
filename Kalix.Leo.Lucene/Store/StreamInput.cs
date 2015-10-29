using Lucene.Net.Store;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Store
{
    public class StreamInput : Stream
    {
        public IndexInput Input { get; set; }

        public StreamInput(IndexInput input)
        {
            Input = input;
        }

        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return true; ; } }
        public override bool CanWrite { get { return false; } }
        public override long Length { get { return Input.Length(); } }

        public override long Position
        {
            get { return Input.FilePointer; }
            set { Input.Seek(value); }
        }

        public override void Flush() { }
        public override Task FlushAsync(CancellationToken ct) { return Task.FromResult(0); }

        public override int Read(byte[] buffer, int offset, int count)
        {
            long pos = Input.FilePointer;
            try
            {
                long len = Input.Length();
                if (count > (len - pos))
                    count = (int)(len - pos);
                Input.ReadBytes(buffer, offset, count);
            }
            catch (Exception) { }
            return (int)(Input.FilePointer - pos);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            var read = Read(buffer, offset, count);
            return Task.FromResult(read);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Input.Seek(offset);
                    break;
                case SeekOrigin.Current:
                    Input.Seek(Input.FilePointer + offset);
                    break;
                case SeekOrigin.End:
                    throw new System.NotImplementedException();
            }
            return Input.FilePointer;
        }

        public override void SetLength(long value)
        {
            throw new System.NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new System.NotImplementedException();
        }

        protected override void Dispose(bool disposing)
        {
            Input.Dispose();
            base.Dispose(disposing);
        }
    }
}
