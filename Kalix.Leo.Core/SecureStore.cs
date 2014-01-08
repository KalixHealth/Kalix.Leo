using Kalix.Leo.Compression;
using Kalix.Leo.Encryption;
using Kalix.Leo.Queue;
using Kalix.Leo.Storage;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class SecureStore : ISecureStore
    {
        private readonly IStore _store;
        private readonly IQueue _backupQueue;
        private readonly IQueue _indexQueue;
        private readonly IEncryptor _encryptor;
        private readonly ICompressor _compressor;

        public SecureStore(IStore store, IQueue backupQueue = null, IQueue indexQueue = null, IEncryptor encryptor = null, ICompressor compressor = null)
        {
            if (_store == null) { throw new ArgumentNullException("writeStore"); }

            _store = store;
            _backupQueue = backupQueue;
            _indexQueue = indexQueue;
            _encryptor = encryptor;
            _compressor = compressor;
        }

        public bool CanEncrypt
        {
            get { return _encryptor != null; }
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

        public IObservable<StoreLocation> FindFiles(string container, string prefix = null)
        {
            return _store.FindFiles(container, prefix);
        }

        public async Task<ObjectWithMetadata<T>> LoadObject<T>(StoreLocation location, string snapshot = null)
        {
            var data = await LoadData(location, snapshot);
            if (!data.Metadata.ContainsKey(MetadataConstants.TypeMetadataKey) || data.Metadata[MetadataConstants.TypeMetadataKey] != typeof(T).FullName)
            {
                throw new InvalidOperationException("Data type does not match metadata");
            }

            var obj = await data.Stream
                .ToArray()
                .Select(b => Encoding.UTF8.GetString(b, 0, b.Length))
                .Select(JsonConvert.DeserializeObject<T>);

            return new ObjectWithMetadata<T>(obj, data.Metadata);
        }

        public async Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null)
        {
            var data = await _store.LoadData(location, snapshot);
            var metadata = data.Metadata;
            var stream = data.Stream;

            // First the decryptor sits on top
            if (metadata.ContainsKey(MetadataConstants.EncryptionMetadataKey))
            {
                if (metadata[MetadataConstants.EncryptionMetadataKey] != _encryptor.Algorithm)
                {
                    throw new InvalidOperationException("Encryption Algorithms do not match, cannot load data");
                }

                stream = _encryptor.Decrypt(stream);
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

            return new DataWithMetadata(stream, metadata);
        }

        public Task<IMetadata> GetMetadata(StoreLocation location, string snapshot = null)
        {
            return _store.GetMetadata(location, snapshot);
        }

        public Task<StoreLocation> SaveObject<T>(StoreLocation location, ObjectWithMetadata<T> obj, IUniqueIdGenerator idGenerator = null, SecureStoreOptions options = SecureStoreOptions.All)
        {
            // Serialise to json as more cross platform
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj.Data)).ToObservable();
            obj.Metadata[MetadataConstants.TypeMetadataKey] = typeof(T).FullName;

            return SaveData(location, new DataWithMetadata(data, obj.Metadata), idGenerator, options);
        }

        public async Task<StoreLocation> SaveData(StoreLocation location, DataWithMetadata data, IUniqueIdGenerator idGenerator = null, SecureStoreOptions options = SecureStoreOptions.All)
        {
            var metadata = new Metadata();
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
                metadata.Add(MetadataConstants.CompressionMetadataKey, _compressor.Algorithm);
            }

            // Next is encryption
            if(options.HasFlag(SecureStoreOptions.Encrypt))
            {
                if(_encryptor == null)
                {
                    throw new ArgumentException("Encrypt option should not be used if no encryptor has been implemented", "options");
                }

                dataStream = _encryptor.Encrypt(dataStream);
                metadata.Add(MetadataConstants.EncryptionMetadataKey, _encryptor.Algorithm);
            }

            /****************************************************
             *  GENERATE ID
             * ***************************************************/
            // Update the location if we do not have an id yet
            if(options.HasFlag(SecureStoreOptions.GenerateId) && !location.Id.HasValue)
            {
                if(idGenerator == null)
                {
                    throw new ArgumentException("GenerateId option should not be used if no idGenerator has been supplied", "options");
                }

                var id = await idGenerator.NextId();
                location = new StoreLocation(location.Container, location.BasePath, id);
            }

            /****************************************************
             *  SAVE METADATA
             * ***************************************************/
            foreach(var m in data.Metadata)
            {
                metadata[m.Key] = m.Value;
            }

            /****************************************************
             *  SAVE THE INITIAL DATA
             * ***************************************************/
            await _store.SaveData(location, new DataWithMetadata(dataStream, metadata));

            /****************************************************
             *  POST SAVE TASKS (SNAPSHOT, BACKUP, INDEX)
             * ***************************************************/
            // The rest of the tasks are done asyncly
            var tasks = new List<Task>();

            if(options.HasFlag(SecureStoreOptions.Backup))
            {
                tasks.Add(_backupQueue.SendMessage(GetMessageDetails(location, metadata)));
            }

            if (options.HasFlag(SecureStoreOptions.Index))
            {
                tasks.Add(_indexQueue.SendMessage(GetMessageDetails(location, metadata)));
            }

            if (tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
            }

            return location;
        }

        public Task Delete(StoreLocation location, SecureStoreOptions options = SecureStoreOptions.All)
        {
            if (options.HasFlag(SecureStoreOptions.KeepDeletes))
            {
                return _store.SoftDelete(location);
            }
            else
            {
                return _store.PermanentDelete(location);
            }
        }

        private string GetMessageDetails(StoreLocation location, IMetadata metadata)
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
