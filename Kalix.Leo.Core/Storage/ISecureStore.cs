using Kalix.Leo.Encryption;
using System;
using System.IO;
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

        Task<Metadata> GetMetadata(StoreLocation location, string snapshot = null);

        Task<string> SaveData(StoreLocation location, Metadata metadata, Func<Stream, Task> savingFunc, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All);
        Task<string> SaveObject<T>(StoreLocation location, ObjectWithMetadata<T> obj, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All);
        Task SaveMetadata(StoreLocation location, Metadata metadata, SecureStoreOptions options = SecureStoreOptions.All);

        Task Delete(StoreLocation location, SecureStoreOptions options = SecureStoreOptions.All);

        IObservable<Snapshot> FindSnapshots(StoreLocation location);
        IObservable<LocationWithMetadata> FindFiles(string container, string prefix = null);

        Task ReIndexAll(string container, Func<LocationWithMetadata, bool> filter, string prefix = null);
        Task BackupAll(string container, string prefix = null);

        Task<IDisposable> Lock(StoreLocation location);
        Task RunOnce(StoreLocation location, Func<Task> action);
        IObservable<bool> RunEvery(StoreLocation location, TimeSpan interval, Action<Exception> unhandledExceptions = null);

        IUniqueIdGenerator GetIdGenerator(StoreLocation location);

        /// <summary>
        /// Make sure a container exists
        /// </summary>
        /// <param name="container">Name of the container to create</param>
        Task CreateContainerIfNotExists(string container);

        /// <summary>
        /// Delete a container if it exists
        /// </summary>
        /// <param name="container">Name of the container to delete</param>
        Task PermanentDeleteContainer(string container);
    }
}
