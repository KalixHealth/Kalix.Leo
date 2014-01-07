using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface ISecureStore
    {
        bool CanEncrypt { get; }
        bool CanCompress { get; }
        bool CanIndex { get; }
        bool CanBackup { get; }

        Task<IDictionary<string, string>> LoadData(Stream writeStream, StoreLocation location, string snapshot = null);
        Task<T> LoadObject<T>(StoreLocation location, string snapshot = null);
        Task<Tuple<T, IDictionary<string, string>>> LoadObjectWithMetadata<T>(StoreLocation location, string snapshot = null);

        Task<IDictionary<string, string>> GetMetadata(StoreLocation location, string snapshot = null);

        Task<StoreLocation> SaveData(Stream data, StoreLocation location, IDictionary<string, string> userMetadata = null, IUniqueIdGenerator idGenerator = null, SecureStoreOptions options = SecureStoreOptions.All);
        Task<StoreLocation> SaveObject<T>(T obj, StoreLocation location, IDictionary<string, string> userMetadata = null, IUniqueIdGenerator idGenerator = null, SecureStoreOptions options = SecureStoreOptions.All);

        Task Delete(StoreLocation location, SecureStoreOptions options = SecureStoreOptions.All);

        Task<IEnumerable<Snapshot>> FindSnapshots(StoreLocation location);
    }
}
