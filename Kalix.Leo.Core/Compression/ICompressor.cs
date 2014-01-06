using System.IO;

namespace Kalix.Leo.Compression
{
    public interface ICompressor
    {
        string Algorithm { get; }

        /// <summary>
        /// Compress should not execute the underlying stream for maximum efficiancy
        /// </summary>
        /// <param name="data">read stream of data to compress</param>
        /// <returns>Read stream which will read from the data stream and compress it</returns>
        Stream Compress(Stream data);

        /// <summary>
        /// Decompress should not execute the underlying stream for maximum efficiancy
        /// </summary>
        /// <param name="data">Write stream to send data that has been decompressed</param>
        /// <returns>Write stream that will decompress data sent to it</returns>
        Stream Decompress(Stream compressedData);
    }
}
