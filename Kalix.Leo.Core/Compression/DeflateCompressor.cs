using Ionic.Zlib;
using System;
using System.IO;

namespace Kalix.Leo.Compression
{
    public class DeflateCompressor : ICompressor
    {
        public string Algorithm
        {
            get { return "deflate"; }
        }

        public Stream Compress(Stream data, bool readMode)
        {
            if(readMode && !data.CanRead)
            {
                throw new ArgumentException("Stream is not readable to compress", "data");
            }

            if (!readMode && !data.CanWrite)
            {
                throw new ArgumentException("Stream is not writable to compress", "data");
            }

            return new DeflateStream(data, CompressionMode.Compress);
        }

        public Stream Decompress(Stream compressedData, bool readMode)
        {
            if (readMode && !compressedData.CanRead)
            {
                throw new ArgumentException("Stream is not readable to decompress", "compressedData");
            }

            if (!readMode && !compressedData.CanWrite)
            {
                throw new ArgumentException("Stream is not writable to decompress", "compressedData");
            }

            return new DeflateStream(compressedData, CompressionMode.Decompress);
        }
    }
}
