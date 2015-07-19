using Kalix.Leo.Compression;
using Kalix.Leo.Encryption;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
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

        public async Task ReIndexAll(string container, Func<LocationWithMetadata, bool> filter, string prefix = null)
        {
            if (_indexQueue == null)
            {
                throw new InvalidOperationException("Index queue has not been defined");
            }

            await FindFiles(container, prefix)
                .Where(filter)
                .SelectMany(f => 
                    Observable.FromAsync(() => _indexQueue.SendMessage(GetMessageDetails(f.Location, f.Metadata)))
                )
                .LastOrDefaultAsync();
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
                )
                .LastOrDefaultAsync();
        }

        public async Task<ObjectWithMetadata<T>> LoadObject<T>(StoreLocation location, string snapshot = null, IEncryptor encryptor = null)
        {
            var data = await LoadData(location, snapshot, encryptor).ConfigureAwait(false);
            if(data == null) { return null; }

            using (var s = data.Stream)
            using (var sr = new StreamReader(s, Encoding.UTF8))
            {
                if(!data.Metadata.ContainsKey(MetadataConstants.TypeMetadataKey))
                {
                    LeoTrace.WriteLine(string.Format("Warning: Data type is not in metadata. expected {0}", typeof(T).FullName));
                }
                else if (data.Metadata[MetadataConstants.TypeMetadataKey] != typeof(T).FullName)
                {
                    LeoTrace.WriteLine(string.Format("Warning: Data type does not match metadata. actual '{0}' vs expected '{1}'", data.Metadata[MetadataConstants.TypeMetadataKey], typeof(T).FullName));
                }

                LeoTrace.WriteLine("Getting data object: " + location);

                var str = await sr.ReadToEndAsync();
                var obj = JsonConvert.DeserializeObject<T>(str);

                LeoTrace.WriteLine("Returning data object: " + location);
                return new ObjectWithMetadata<T>(obj, data.Metadata);
            }
        }

        public async Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null, IEncryptor encryptor = null)
        {
            var data = await _store.LoadData(location, snapshot).ConfigureAwait(false);
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

                stream = encryptor.Decrypt(stream, true);
            }

            // Might need to decompress too!
            if (metadata.ContainsKey(MetadataConstants.CompressionMetadataKey))
            {
                if (metadata[MetadataConstants.CompressionMetadataKey] != _compressor.Algorithm)
                {
                    throw new InvalidOperationException("Compression Algorithms do not match, cannot load data");
                }

                stream = _compressor.Decompress(stream, true);
            }

            return new DataWithMetadata(stream, metadata);
        }

        public Task<Metadata> GetMetadata(StoreLocation location, string snapshot = null)
        {
            return _store.GetMetadata(location, snapshot);
        }

        public Task<Metadata> SaveObject<T>(StoreLocation location, ObjectWithMetadata<T> obj, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All)
        {
            // Serialise to json as more cross platform
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj.Data));
            obj.Metadata[MetadataConstants.TypeMetadataKey] = typeof(T).FullName;

            return SaveData(location, obj.Metadata, (s) => s.WriteAsync(data, 0, data.Length), encryptor, options);
        }

        public async Task<Metadata> SaveData(StoreLocation location, Metadata mdata, Func<Stream, Task> savingFunc, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All)
        {
            LeoTrace.WriteLine("Saving: " + location.Container + ", " + location.BasePath + ", " + (location.Id.HasValue ? location.Id.Value.ToString() : "null"));
            var metadata = new Metadata(mdata);

            /****************************************************
             *  SETUP METADATA
             * ***************************************************/
            if (encryptor != null)
            {
                metadata[MetadataConstants.EncryptionMetadataKey] = encryptor.Algorithm;
            }
            else
            {
                metadata.Remove(MetadataConstants.EncryptionMetadataKey);
            }

            if (options.HasFlag(SecureStoreOptions.Compress))
            {
                if (_compressor == null)
                {
                    throw new ArgumentException("Compression option should not be used if no compressor has been implemented", "options");
                }
                metadata[MetadataConstants.CompressionMetadataKey] = _compressor.Algorithm;
            }
            else
            {
                metadata.Remove(MetadataConstants.CompressionMetadataKey);
            }

            /****************************************************
             *  PREPARE THE SAVE STREAM
             * ***************************************************/
            var m = await _store.SaveData(location, metadata, async (s) =>
            {
                Stream encStream = null;
                Stream compStream = null;

                try
                {
                    // Encrypt just before writing to the stream (if we need)
                    if (encryptor != null)
                    {
                        encStream = encryptor.Encrypt(s, false);
                    }

                    // Compression comes right before encryption
                    if(options.HasFlag(SecureStoreOptions.Compress))
                    {
                        compStream = _compressor.Compress(encStream ?? s, false);
                    }

                    // Work out which stream we are actually writing to...
                    using (var writeStream = new LengthCounterStream(compStream ?? encStream ?? s))
                    {
                        await savingFunc(writeStream).ConfigureAwait(false);
                        return writeStream.Length;
                    }
                }
                finally
                {
                    if (compStream != null) { compStream.Dispose(); }
                    if (encStream != null) { encStream.Dispose(); }
                }
            }).ConfigureAwait(false);

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
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }

            return m;
        }

        public async Task<Metadata> SaveMetadata(StoreLocation location, Metadata metadata, SecureStoreOptions options = SecureStoreOptions.All)
        {
            var m = await _store.SaveMetadata(location, metadata).ConfigureAwait(false);

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
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }

            return m;
        }

        public async Task Delete(StoreLocation location, SecureStoreOptions options = SecureStoreOptions.All)
        {
            var metadata = await _store.GetMetadata(location).ConfigureAwait(false);
            if (metadata == null) { return; }

            if (options.HasFlag(SecureStoreOptions.KeepDeletes))
            {
                await _store.SoftDelete(location).ConfigureAwait(false);
            }
            else
            {
                await _store.PermanentDelete(location).ConfigureAwait(false);
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
                await Task.WhenAll(tasks).ConfigureAwait(false);
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

        public IObservable<bool> RunEvery(StoreLocation location, TimeSpan interval, Action<Exception> unhandledExceptions = null)
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
