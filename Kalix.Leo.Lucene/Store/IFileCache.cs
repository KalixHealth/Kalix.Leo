using Kalix.Leo.Storage;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Store
{
    /// <summary>
    /// Cache to store files that will read/written from, when disposed should drop the directory
    /// </summary>
    public interface IFileCache : IDisposable
    {
        /// <summary>
        /// Updates a cached file by reading the data, if null then it will create a new file.
        /// Metadata does not need to be saved...
        /// </summary>
        /// <param name="key">Relative path the file</param>
        /// <param name="data">Data to save, can be null if we need to create a new empty cache file</param>
        Task<bool> UpdateIfModified(string key, Task<DataWithMetadata> data = null);

        /// <summary>
        /// Get a stream and metadata from the file
        /// </summary>
        /// <param name="key">Relative path to the file</param>
        /// <returns>Metadata and the data stream</returns>
        Task<DataWithMetadata> LoadAllData(string key);

        /// <summary>
        /// Gets a stream than can be used for reading or writing (not at the same time though)
        /// </summary>
        /// <param name="key">Relative path to the file</param>
        /// <param name="initialPosition">Initial position that the stream should be pointing at</param>
        /// <returns>Stream to read/write data from</returns>
        Task<Stream> GetReadWriteStream(string key, long initialPosition = 0);

        /// <summary>
        /// Gets a readonly stream, should be able to open stream on a file that had the readwrite method called on it...
        /// </summary>
        /// <param name="key">Relative path to the file</param>
        /// <param name="initialPosition">Initial position that the stream should be pointing at</param>
        /// <returns>Stream to read data from</returns>
        Task<Stream> GetReadonlyStream(string key, long initialPosition = 0);

        /// <summary>
        /// Remove a file
        /// </summary>
        /// <param name="key">Relative path to cache the data</param>
        Task Delete(string key);
    }
}
