using Lucene.Net.Store;
using System;
using System.IO;

namespace Kalix.Leo.Lucene.Store
{
    public class StreamOutput : Stream
    {
        public IndexOutput Output { get; set; }

        public StreamOutput(IndexOutput output)
        {
            Output = output;
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
            Output.Flush();
        }

        public override long Length
        {
            get { return Output.Length; }
        }

        public override long Position
        {
            get
            {
                return Output.FilePointer;
            }
            set
            {
                Output.Seek(value);
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
                    Output.Seek(offset);
                    break;
                case SeekOrigin.Current:
                    Output.Seek(Output.FilePointer + offset);
                    break;
                case SeekOrigin.End:
                    throw new System.NotImplementedException();
            }
            return Output.FilePointer;
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            Output.WriteBytes(buffer, offset, count);
        }

        protected override void Dispose(bool disposing)
        {
            Output.Flush();
            Output.Dispose();

            base.Dispose(disposing);
        }
    }
}
