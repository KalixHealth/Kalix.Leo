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
        /// <param name="data">Write Stream to send compressed data</param>
        /// <returns>Write Stream that will compress data</returns>
        Stream CompressWriteStream(Stream data);

        /// <summary>
        /// Decompress a stream of compressed data
        /// </summary>
        /// <param name="compressedData">Stream of data that needs to be decompressed</param>
        /// <returns>Stream of data that is decompressed</returns>
        Stream DecompressReadStream(Stream compressedData);
    }
}
