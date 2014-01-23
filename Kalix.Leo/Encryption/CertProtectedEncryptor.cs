using Kalix.ApiCrypto.AES;
using Kalix.ApiCrypto.RSA;
using Kalix.Leo.Storage;
using System;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Encryption
{
    public class CertProtectedEncryptor : IEncryptor
    {
        private readonly Lazy<Task<AESEncryptor>> _encryptor;

        public CertProtectedEncryptor(IStore store, StoreLocation keyLocation, RSAServiceProvider rsaCert)
        {
            _encryptor = new Lazy<Task<AESEncryptor>>(async () =>
            {
                var data = await store.LoadData(keyLocation);
                var blob = await data.Stream.SelectMany(s => s).ToArray();
                return AESBlob.CreateEncryptor(blob, rsaCert);
            });
        }

        public string Algorithm
        {
            get { return "AES"; }
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
