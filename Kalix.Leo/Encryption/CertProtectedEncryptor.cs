using AsyncBridge;
using Kalix.ApiCrypto.AES;
using Kalix.ApiCrypto.RSA;
using Kalix.Leo.Storage;
using System;
using System.IO;
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
                AESEncryptor aes = null;
                using (var w = AsyncHelper.Wait)
                {
                    w.Run(CreateEncryptor(store, keyLocation, rsaCert), a => aes = a);
                }
                return aes;
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
                await store.SaveData(keyLocation, null, (s) => s.WriteAsync(blob, 0, blob.Length)).ConfigureAwait(false);
            }
            else
            {
                using (var ms = new MemoryStream())
                {
                    await data.Stream.CopyToAsync(ms).ConfigureAwait(false);
                    blob = ms.ToArray();
                }
            }

            return AESBlob.CreateEncryptor(blob, rsaCert);
        }
    }
}
