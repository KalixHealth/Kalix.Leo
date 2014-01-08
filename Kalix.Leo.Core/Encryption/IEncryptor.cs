using System;

namespace Kalix.Leo.Encryption
{
    public interface IEncryptor
    {
        string Algorithm { get; }

        /// <summary>
        /// Encrypt a stream of data
        /// </summary>
        /// <param name="data">stream of data to encrypt</param>
        /// <returns>stream of encrypted data</returns>
        IObservable<byte> Encrypt(IObservable<byte> data);

        /// <summary>
        /// Decrypt a stream of encrypted data
        /// </summary>
        /// <param name="data">Stream of data to decrypt</param>
        /// <returns>Stream of data</returns>
        IObservable<byte> Decrypt(IObservable<byte> encyptedData);
    }
}
