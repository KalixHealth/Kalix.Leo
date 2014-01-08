using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface ISecureStore
    {
        bool CanEncrypt { get; }
        bool CanCompress { get; }
        bool CanIndex { get; }
        bool CanBackup { get; }

        Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null);
        Task<ObjectWithMetadata<T>> LoadObject<T>(StoreLocation location, string snapshot = null);

        Task<IMetadata> GetMetadata(StoreLocation location, string snapshot = null);

        Task<StoreLocation> SaveData(StoreLocation location, DataWithMetadata data, IUniqueIdGenerator idGenerator = null, SecureStoreOptions options = SecureStoreOptions.All);
        Task<StoreLocation> SaveObject<T>(StoreLocation location, ObjectWithMetadata<T> obj, IUniqueIdGenerator idGenerator = null, SecureStoreOptions options = SecureStoreOptions.All);

        Task Delete(StoreLocation location, SecureStoreOptions options = SecureStoreOptions.All);

        IObservable<Snapshot> FindSnapshots(StoreLocation location);
        IObservable<StoreLocation> FindFiles(string container, string prefix = null);
    }
}
