using Kalix.Leo.Encryption;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Storage
{
    public interface ISecureStore
    {
        bool CanCompress { get; }
        bool CanIndex { get; }
        bool CanBackup { get; }

        Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null, IEncryptor encryptor = null);
        Task<ObjectWithMetadata<T>> LoadObject<T>(StoreLocation location, string snapshot = null, IEncryptor encryptor = null);

        Task<IMetadata> GetMetadata(StoreLocation location, string snapshot = null);

        Task<StoreLocation> SaveData(StoreLocation location, DataWithMetadata data, IUniqueIdGenerator idGenerator = null, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All);
        Task<StoreLocation> SaveObject<T>(StoreLocation location, ObjectWithMetadata<T> obj, IUniqueIdGenerator idGenerator = null, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All);

        Task Delete(StoreLocation location, SecureStoreOptions options = SecureStoreOptions.All);

        IObservable<Snapshot> FindSnapshots(StoreLocation location);
        IObservable<LocationWithMetadata> FindFiles(string container, string prefix = null);

        Task ReIndexAll(string container, string prefix = null);
        Task BackupAll(string container, string prefix = null);

        Task<IDisposable> Lock(StoreLocation location);

        IUniqueIdGenerator GetIdGenerator(StoreLocation location);
    }
}
