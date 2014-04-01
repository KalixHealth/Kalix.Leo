using System;

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
        /// <returns>stream of encrypted data</returns>
        IObservable<byte[]> Encrypt(IObservable<byte[]> data);

        /// <summary>
        /// Decrypt a stream of encrypted data
        /// </summary>
        /// <param name="encyptedData">Stream of data to decrypt</param>
        /// <returns>Stream of data</returns>
        IObservable<byte[]> Decrypt(IObservable<byte[]> encyptedData);
    }
}
