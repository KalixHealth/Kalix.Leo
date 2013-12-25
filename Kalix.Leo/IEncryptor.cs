using System.IO;

namespace Kalix.Leo
{
    public interface IEncryptor
    {
        /// <summary>
        /// Encrypt should not execute the underlying stream for maximum efficiancy
        /// </summary>
        /// <param name="data">read stream of data to encrypt</param>
        /// <returns>Read stream which will read from the data stream and encrypt it</returns>
        Stream Encrypt(Stream data);

        /// <summary>
        /// Decrypt should not execute the underlying stream for maximum efficiancy
        /// </summary>
        /// <param name="data">read stream of data to decrypt</param>
        /// <returns>Read stream which will read from the data stream and decrypt it</returns>
        Stream Decrypt(Stream encyptedData);
    }
}
