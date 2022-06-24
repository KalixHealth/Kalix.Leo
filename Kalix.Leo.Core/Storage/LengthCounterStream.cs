using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Storage;

public class LengthCounterStream : Stream
{
    private readonly Stream _stream;
    private long _length = 0;

    public LengthCounterStream(Stream stream)
    {
        _stream = stream;
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _length += count;
        _stream.Write(buffer, offset, count);
    }

    public override void Flush()
    {
        _stream.Flush();
    }

    public override Task FlushAsync(CancellationToken ct)
    {
        return _stream.FlushAsync(ct);
    }

    protected override void Dispose(bool disposing)
    {
        _stream.Dispose();
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

    public override long Length
    {
        get { return _length; }
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