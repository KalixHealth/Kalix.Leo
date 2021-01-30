using Kalix.ApiCrypto.AES;
using Kalix.Leo.Storage;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Encryption
{
    public class CertProtectedEncryptor : IEncryptor
    {
        private const AESKeySize DefaultKeySize = AESKeySize.AES256;

        private readonly AESEncryptor _encryptor;
        private readonly string _partition;

        public static async Task<IEncryptor> CreateEncryptor(IOptimisticStore store, StoreLocation keyLocation, RSA rsaCert)
        {
            bool isFound;
            byte[] blob;
            do
            {
                var data = await store.LoadData(keyLocation);
                if (data == null)
                {
                    // Have to create a new key
                    blob = AESBlob.CreateBlob(DefaultKeySize, rsaCert);
                    var ct = CancellationToken.None;

                    // We use an optimistic write so that it will only create the file IF THE FILE DOES NOT EXIST
                    // This will catch rare cases where two server calls may try to create two keys
                    var result = await store.TryOptimisticWrite(keyLocation, null, null, async (s) =>
                    {
                        await s.WriteAsync(blob, 0, blob.Length, ct);
                        return blob.Length;
                    }, ct);
                    isFound = result.Result;
                }
                else
                {
                    blob = await data.Stream.ReadBytes();
                    isFound = true;
                }
            } while (!isFound);

            var encryptor = AESBlob.CreateEncryptor(blob, rsaCert);
            return new CertProtectedEncryptor(keyLocation.Container, encryptor);
        }

        private CertProtectedEncryptor(string partition, AESEncryptor encryptor)
        {
            _partition = partition;
            _encryptor = encryptor;
        }

        public string Algorithm
        {
            get { return "AES_" + _partition; }
        }

        public Stream Encrypt(Stream data, bool readMode)
        {
            return _encryptor.Encrypt(data, readMode);
        }

        public Stream Decrypt(Stream encyptedData, bool readMode)
        {
            return _encryptor.Decrypt(encyptedData, readMode);
        }
    }
}
