using Kalix.Leo.Compression;
using Kalix.Leo.Encryption;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kalix.Leo.Storage
{
    public class SecureStore : ISecureStore
    {
        private readonly IOptimisticStore _store;
        private readonly IQueue _backupQueue;
        private readonly IQueue _indexQueue;
        private readonly ICompressor _compressor;

        public SecureStore(IOptimisticStore store, IQueue backupQueue = null, IQueue indexQueue = null, ICompressor compressor = null)
        {
            if (store == null) { throw new ArgumentNullException("store"); }

            _store = store;
            _backupQueue = backupQueue;
            _indexQueue = indexQueue;
            _compressor = compressor;
        }

        public bool CanCompress
        {
            get { return _compressor != null; }
        }

        public bool CanIndex
        {
            get { return _indexQueue != null; }
        }

        public bool CanBackup
        {
            get { return _backupQueue != null; }
        }

        public IObservable<Snapshot> FindSnapshots(StoreLocation location)
        {
            return _store.FindSnapshots(location);
        }

        public IObservable<LocationWithMetadata> FindFiles(string container, string prefix = null)
        {
            return _store.FindFiles(container, prefix);
        }

        public async Task ReIndexAll(string container, string prefix = null)
        {
            if (_indexQueue == null)
            {
                throw new InvalidOperationException("Index queue has not been defined");
            }

            await FindFiles(container, prefix)
                .SelectMany(f => 
                    Observable.FromAsync(() => _indexQueue.SendMessage(GetMessageDetails(f.Location, f.Metadata)))
                );
        }

        public async Task BackupAll(string container, string prefix = null)
        {
            if (_backupQueue == null)
            {
                throw new InvalidOperationException("Backup queue has not been defined");
            }

            await FindFiles(container, prefix)
                .SelectMany(f =>
                    Observable.FromAsync(() => _backupQueue.SendMessage(GetMessageDetails(f.Location, f.Metadata)))
                );
        }

        public async Task<ObjectWithMetadata<T>> LoadObject<T>(StoreLocation location, string snapshot = null, IEncryptor encryptor = null)
        {
            var data = await LoadData(location, snapshot, encryptor);
            if(data == null) { return null; }

            using (data)
            {
                if(!data.Metadata.ContainsKey(MetadataConstants.TypeMetadataKey))
                {
                    LeoTrace.TraceAction(string.Format("Warning: Data type is not in metadata. expected {0}", typeof(T).FullName));
                }
                else if (data.Metadata[MetadataConstants.TypeMetadataKey] != typeof(T).FullName)
                {
                    LeoTrace.TraceAction(string.Format("Warning: Data type does not match metadata. actual '{0}' vs expected '{1}'", data.Metadata[MetadataConstants.TypeMetadataKey], typeof(T).FullName));
                }

                var obj = await data.Stream
                    .ToList()
                    .Select(b =>
                    {
                        var all = b.SelectMany(a => a).ToArray();
                        return Encoding.UTF8.GetString(all, 0, all.Length);
                    })
                    .Select(JsonConvert.DeserializeObject<T>);

                return new ObjectWithMetadata<T>(obj, data.Metadata);
            }
        }

        public async Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null, IEncryptor encryptor = null)
        {
            var data = await _store.LoadData(location, snapshot);
            if (data == null) { return null; }

            var metadata = data.Metadata;
            var stream = data.Stream;

            // First the decryptor sits on top
            if (metadata.ContainsKey(MetadataConstants.EncryptionMetadataKey))
            {
                if (metadata[MetadataConstants.EncryptionMetadataKey] != encryptor.Algorithm)
                {
                    throw new InvalidOperationException("Encryption Algorithms do not match, cannot load data");
                }

                stream = encryptor.Decrypt(stream);
            }

            // Might need to decompress too!
            if (metadata.ContainsKey(MetadataConstants.CompressionMetadataKey))
            {
                if (metadata[MetadataConstants.CompressionMetadataKey] != _compressor.Algorithm)
                {
                    throw new InvalidOperationException("Compression Algorithms do not match, cannot load data");
                }

                stream = _compressor.Decompress(stream);
            }

            return new DataWithMetadata(stream, metadata, () => data.Dispose());
        }

        public Task<Metadata> GetMetadata(StoreLocation location, string snapshot = null)
        {
            return _store.GetMetadata(location, snapshot);
        }

        public Task SaveObject<T>(StoreLocation location, ObjectWithMetadata<T> obj, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All)
        {
            // Serialise to json as more cross platform
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj.Data));
            obj.Metadata[MetadataConstants.TypeMetadataKey] = typeof(T).FullName;

            return SaveData(location, new DataWithMetadata(Observable.Return(data), obj.Metadata), encryptor, options);
        }

        public async Task SaveData(StoreLocation location, DataWithMetadata data, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All)
        {
            var metadata = new Metadata(data.Metadata);
            var dataStream = data.Stream;

            /****************************************************
             *  PREPARE THE STREAM
             * ***************************************************/
            // Data is a read stream, lets layer like an an onion :)
            // First is compression
            if(options.HasFlag(SecureStoreOptions.Compress))
            {
                if(_compressor == null)
                {
                    throw new ArgumentException("Compression option should not be used if no compressor has been implemented", "options");
                }

                dataStream = _compressor.Compress(dataStream);
                metadata[MetadataConstants.CompressionMetadataKey] = _compressor.Algorithm;
            }
            else
            {
                // Make sure to remove to not confuse loaders!
                metadata.Remove(MetadataConstants.CompressionMetadataKey);
            }

            // Next is encryption
            if(encryptor != null)
            {
                dataStream = encryptor.Encrypt(dataStream);
                metadata[MetadataConstants.EncryptionMetadataKey] = encryptor.Algorithm;
            }
            else
            {
                // Make sure to remove to not confuse loaders!
                metadata.Remove(MetadataConstants.EncryptionMetadataKey);
            }

            /****************************************************
             *  SAVE THE INITIAL DATA
             * ***************************************************/
            await _store.SaveData(location, new DataWithMetadata(dataStream, metadata));

            /****************************************************
             *  POST SAVE TASKS (BACKUP, INDEX)
             * ***************************************************/
            // The rest of the tasks are done asyncly
            var tasks = new List<Task>();

            if(options.HasFlag(SecureStoreOptions.Backup))
            {
                if(_backupQueue == null)
                {
                    throw new ArgumentException("Backup option should not be used if no backup queue has been defined", "options");
                }

                tasks.Add(_backupQueue.SendMessage(GetMessageDetails(location, metadata)));
            }

            if (options.HasFlag(SecureStoreOptions.Index))
            {
                if (_indexQueue == null)
                {
                    throw new ArgumentException("Index option should not be used if no index queue has been defined", "options");
                }

                tasks.Add(_indexQueue.SendMessage(GetMessageDetails(location, metadata)));
            }

            if (tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
            }
        }

        public async Task SaveMetadata(StoreLocation location, Metadata metadata, SecureStoreOptions options = SecureStoreOptions.All)
        {
            await _store.SaveMetadata(location, metadata);

            /****************************************************
             *  POST SAVE TASKS (BACKUP, INDEX)
             * ***************************************************/
            // The rest of the tasks are done asyncly
            var tasks = new List<Task>();

            if (options.HasFlag(SecureStoreOptions.Backup))
            {
                if (_backupQueue == null)
                {
                    throw new ArgumentException("Backup option should not be used if no backup queue has been defined", "options");
                }

                tasks.Add(_backupQueue.SendMessage(GetMessageDetails(location, metadata)));
            }

            if (options.HasFlag(SecureStoreOptions.Index))
            {
                if (_indexQueue == null)
                {
                    throw new ArgumentException("Index option should not be used if no index queue has been defined", "options");
                }

                tasks.Add(_indexQueue.SendMessage(GetMessageDetails(location, metadata)));
            }

            if (tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
            }
        }

        public async Task Delete(StoreLocation location, SecureStoreOptions options = SecureStoreOptions.All)
        {
            var metadata = await _store.GetMetadata(location);
            if (metadata == null) { return; }

            if (options.HasFlag(SecureStoreOptions.KeepDeletes))
            {
                await _store.SoftDelete(location);
            }
            else
            {
                await _store.PermanentDelete(location);
            }

            // The rest of the tasks are done asyncly
            var tasks = new List<Task>();

            if (options.HasFlag(SecureStoreOptions.Backup))
            {
                if (_backupQueue == null)
                {
                    throw new ArgumentException("Backup option should not be used if no backup queue has been defined", "options");
                }

                tasks.Add(_backupQueue.SendMessage(GetMessageDetails(location, metadata)));
            }

            if (options.HasFlag(SecureStoreOptions.Index))
            {
                if (_indexQueue == null)
                {
                    throw new ArgumentException("Index option should not be used if no index queue has been defined", "options");
                }

                tasks.Add(_indexQueue.SendMessage(GetMessageDetails(location, metadata)));
            }

            if (tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
            }
        }

        public Task<IDisposable> Lock(StoreLocation location)
        {
            return _store.Lock(location);
        }

        public Task RunOnce(StoreLocation location, Func<Task> action)
        {
            return _store.RunOnce(location, action);
        }

        public IObservable<Unit> RunEvery(StoreLocation location, TimeSpan interval, Action<Exception> unhandledExceptions = null)
        {
            return _store.RunEvery(location, interval, unhandledExceptions);
        }

        public IUniqueIdGenerator GetIdGenerator(StoreLocation location)
        {
            return new UniqueIdGenerator(_store, location);
        }

        /// <summary>
        /// Make sure a container exists
        /// </summary>
        /// <param name="container">Name of the container to create</param>
        public Task CreateContainerIfNotExists(string container)
        {
            return _store.CreateContainerIfNotExists(container);
        }

        /// <summary>
        /// Delete a container if it exists
        /// </summary>
        /// <param name="container">Name of the container to delete</param>
        public Task PermanentDeleteContainer(string container)
        {
            return _store.PermanentDeleteContainer(container);
        }

        private string GetMessageDetails(StoreLocation location, Metadata metadata)
        {
            var details = new StoreDataDetails
            {
                Container = location.Container,
                BasePath = location.BasePath,
                Id = location.Id,
                Metadata = metadata
            };
            return JsonConvert.SerializeObject(details);
        }
    }
}
