using System;
using System.IO;

namespace Kalix.Leo.Compression
{
    /// <summary>
    /// An object that can compress/decompress a stream of data
    /// </summary>
    public interface ICompressor
    {
        /// <summary>
        /// The algorithm used by this particular compressor
        /// </summary>
        string Algorithm { get; }

        /// <summary>
        /// Compress a stream of data
        /// </summary>
        /// <param name="data">Stream of data to compress</param>
        /// <param name="readMode">Whether we are working with a read or write stream</param>
        /// <returns>Stream of data that will be compressed</returns>
        Stream Compress(Stream data, bool readMode);

        /// <summary>
        /// Decompress a stream of compressed data
        /// </summary>
        /// <param name="compressedData">Stream of compressed data to decompress</param>
        /// <param name="readMode">Whether we are working with a read or write stream</param>
        /// <returns>Stream of data</returns>
        Stream Decompress(Stream compressedData, bool readMode);
    }
}
