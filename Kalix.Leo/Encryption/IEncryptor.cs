using System.IO;

namespace Kalix.Leo.Encryption
{
    public interface IEncryptor
    {
        string Algorithm { get; }

        /// <summary>
        /// Encrypt should not execute the underlying stream for maximum efficiancy
        /// </summary>
        /// <param name="data">read stream of data to encrypt</param>
        /// <returns>Read stream which will read from the data stream and encrypt it</returns>
        Stream Encrypt(Stream data);

        /// <summary>
        /// Decrypt should not execute the underlying stream for maximum efficiancy
        /// </summary>
        /// <param name="data">Write stream to send data that has been decrypted</param>
        /// <returns>Write stream that will decrypt data sent to it</returns>
        Stream Decrypt(Stream encyptedData);
    }
}
