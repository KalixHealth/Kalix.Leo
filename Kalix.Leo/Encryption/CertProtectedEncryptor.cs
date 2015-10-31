using Kalix.ApiCrypto.AES;
using Kalix.ApiCrypto.RSA;
using Kalix.Leo.Storage;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Encryption
{
    public class CertProtectedEncryptor : IEncryptor
    {
        private const AESKeySize DefaultKeySize = AESKeySize.AES256;

        private readonly Lazy<AESEncryptor> _encryptor;
        private readonly string _partition;

        public CertProtectedEncryptor(IStore store, StoreLocation keyLocation, RSAServiceProvider rsaCert)
        {
            _partition = keyLocation.Container;
            _encryptor = new Lazy<AESEncryptor>(() =>
            {
                try
                {
                    return CreateEncryptor(store, keyLocation, rsaCert).Result;
                }
                catch (AggregateException ex)
                {
                    throw ex.InnerException;
                }
            });
        }

        public string Algorithm
        {
            get { return "AES_" + _partition; }
        }

        public Stream Encrypt(Stream data, bool readMode)
        {
            return _encryptor.Value.Encrypt(data, readMode);
        }

        public Stream Decrypt(Stream encyptedData, bool readMode)
        {
            return _encryptor.Value.Decrypt(encyptedData, readMode);
        }

        private static async Task<AESEncryptor> CreateEncryptor(IStore store, StoreLocation keyLocation, RSAServiceProvider rsaCert)
        {
            var data = await store.LoadData(keyLocation).ConfigureAwait(false);
            byte[] blob;
            if (data == null)
            {
                // Have to create a new key
                blob = AESBlob.CreateBlob(DefaultKeySize, rsaCert);
                var ct = CancellationToken.None;
                await store.SaveData(keyLocation, null, async (s) =>
                {
                    await s.WriteAsync(blob, 0, blob.Length, ct).ConfigureAwait(false);
                    return blob.Length;
                }, ct).ConfigureAwait(false);
            }
            else
            {
                blob = await data.Stream.ReadBytes().ConfigureAwait(false);
            }

            return AESBlob.CreateEncryptor(blob, rsaCert);
        }
    }
}
