using Kalix.ApiCrypto.AES;
using Kalix.ApiCrypto.RSA;
using Kalix.Leo.Storage;
using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Encryption
{
    public class CertProtectedEncryptor : IEncryptor
    {
        private const AESKeySize DefaultKeySize = AESKeySize.AES256;

        private readonly Lazy<Task<AESEncryptor>> _encryptor;
        private readonly string _partition;

        public CertProtectedEncryptor(IStore store, StoreLocation keyLocation, RSAServiceProvider rsaCert)
        {
            _partition = keyLocation.Container;
            _encryptor = new Lazy<Task<AESEncryptor>>(async () =>
            {
                var data = await store.LoadData(keyLocation).ConfigureAwait(false);
                byte[] blob;
                if (data == null)
                {
                    // Have to create a new key
                    blob = AESBlob.CreateBlob(DefaultKeySize, rsaCert);
                    await store.SaveData(keyLocation, new DataWithMetadata(Observable.Return(blob, TaskPoolScheduler.Default))).ConfigureAwait(false);
                }
                else
                {
                    blob = await data.Stream.SelectMany(s => s).ToArray();
                }
                return AESBlob.CreateEncryptor(blob, rsaCert);
            });
        }

        public string Algorithm
        {
            get { return "AES_" + _partition; }
        }

        public IObservable<byte[]> Encrypt(IObservable<byte[]> data)
        {
            return Observable.FromAsync(() => _encryptor.Value)
                .SelectMany(e => e.Encrypt(data));
        }

        public IObservable<byte[]> Decrypt(IObservable<byte[]> encyptedData)
        {
            return Observable.FromAsync(() => _encryptor.Value)
                .SelectMany(e => e.Decrypt(encyptedData));
        }
    }
}
