using System.IO;

namespace Kalix.Leo.Encryption
{
    /// <summary>
    /// An object that is able to encrypt and decrypt data
    /// </summary>
    public interface IEncryptor
    {
        /// <summary>
        /// A name of the algorithm this encrytor uses, used to match algorithms at decryption time
        /// </summary>
        string Algorithm { get; }

        /// <summary>
        /// Encrypt a stream of data
        /// </summary>
        /// <param name="data">stream of data to encrypt</param>
        /// <param name="readMode">Whether this is a read or a write stream</param>
        /// <returns>stream of encrypted data</returns>
        Stream Encrypt(Stream data, bool readMode);

        /// <summary>
        /// Decrypt a stream of encrypted data
        /// </summary>
        /// <param name="encyptedData">Stream of data to decrypt</param>
        /// <param name="readMode">Whether this is a read or a write stream</param>
        /// <returns>Stream of data</returns>
        Stream Decrypt(Stream encyptedData, bool readMode);
    }
}
