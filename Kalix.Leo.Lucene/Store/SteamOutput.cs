using Lucene.Net.Store;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Store
{
    public class StreamOutput : Stream
    {
        private IndexOutput _output { get; set; }

        public StreamOutput(IndexOutput output)
        {
            _output = output;
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
            _output.Flush();
        }

        public override Task FlushAsync(CancellationToken ct)
        {
            _output.Flush();
            return Task.FromResult(0);
        }

        public override long Length
        {
            get { return _output.Length; }
        }

        public override long Position
        {
            get
            {
                return _output.FilePointer;
            }
            set
            {
                _output.Seek(value);
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    _output.Seek(offset);
                    break;
                case SeekOrigin.Current:
                    _output.Seek(_output.FilePointer + offset);
                    break;
                case SeekOrigin.End:
                    throw new System.NotImplementedException();
            }
            return _output.FilePointer;
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _output.WriteBytes(buffer, offset, count);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            _output.WriteBytes(buffer, offset, count);
            return Task.FromResult(0);
        }

        protected override void Dispose(bool disposing)
        {
            _output.Flush();
            _output.Dispose();

            base.Dispose(disposing);
        }
    }
}
