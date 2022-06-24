using System;
using System.IO;
using System.IO.Compression;

namespace Kalix.Leo.Compression;

public class DeflateCompressor : ICompressor
{
    public string Algorithm
    {
        get { return "deflate"; }
    }

    public Stream CompressWriteStream(Stream data)
    {
        if (!data.CanWrite)
        {
            throw new ArgumentException("Stream is not writable to compress", nameof(data));
        }

        return new DeflateStream(data, CompressionMode.Compress);
    }

    public Stream DecompressReadStream(Stream compressedData)
    {
        if (!compressedData.CanRead)
        {
            throw new ArgumentException("Stream is not readable to decompress", nameof(compressedData));
        }

        return new DeflateStream(compressedData, CompressionMode.Decompress);
    }
}