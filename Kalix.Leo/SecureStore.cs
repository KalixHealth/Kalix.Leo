using Kalix.Leo.Compression;
using Kalix.Leo.Encryption;
using Kalix.Leo.Queue;
using Kalix.Leo.Storage;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class SecureStore
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

        public IUniqueIdGenerator CreateUniqueIdGenerator(StoreLocation location, int rangeSize = 10, int maxRetries = 25)
        {
            return new UniqueIdGenerator(_store, location, rangeSize, maxRetries);
        }

        public Task<IEnumerable<Snapshot>> FindSnapshots(StoreLocation location)
        {
            return _store.FindSnapshots(location);
        }

        public async Task<Tuple<T, IDictionary<string, string>>> LoadObjectWithMetadata<T>(StoreLocation location, string snapshot = null)
        {
            using(var ms = new MemoryStream())
            {
                var metadata = await LoadData(ms, location, snapshot);
                if (!metadata.ContainsKey(MetadataConstants.TypeMetadataKey) || metadata[MetadataConstants.TypeMetadataKey] != typeof(T).FullName)
                {
                    throw new InvalidOperationException("Data type does not match metadata");
                }

                var strBytes = ms.ToArray();
                var data = JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(strBytes, 0, strBytes.Length));

                return Tuple.Create(data, metadata);
            }
        }

        public Task<T> LoadObject<T>(StoreLocation location, string snapshot = null)
        {
            return LoadObjectWithMetadata<T>(location, snapshot).ContinueWith(t => t.Result.Item1);
        }

        public async Task<IDictionary<string, string>> LoadData(Stream writeStream, StoreLocation location, string snapshot = null)
        {
            IDictionary<string, string> metadata = null;
            
            await _store.LoadData(location, m =>
            {
                metadata = m ?? new Dictionary<string, string>();

                // First the decryptor sits on top
                if (metadata.ContainsKey(MetadataConstants.EncryptionMetadataKey))
                {
                    if (metadata[MetadataConstants.EncryptionMetadataKey] != _encryptor.Algorithm)
                    {
                        throw new InvalidOperationException("Encryption Algorithms do not match, cannot load data");
                    }

                    writeStream = _encryptor.Decrypt(writeStream);
                }

                // Might need to decompress first too!
                if (metadata.ContainsKey(MetadataConstants.CompressionMetadataKey))
                {
                    if (metadata[MetadataConstants.CompressionMetadataKey] != _compressor.Algorithm)
                    {
                        throw new InvalidOperationException("Compression Algorithms do not match, cannot load data");
                    }

                    writeStream = _compressor.Decompress(writeStream);
                }

                return writeStream;
            }, snapshot);

            // If no file then metadata is null
            return metadata;
        }

        public Task<StoreLocation> SaveObject<T>(T obj, StoreLocation location, IDictionary<string, string> userMetadata = null, IUniqueIdGenerator idGenerator = null, SecureStoreOptions options = SecureStoreOptions.All)
        {
            // Serialise to json as more cross platform
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj));
            using(var ms = new MemoryStream(data))
            {
                userMetadata = userMetadata ?? new Dictionary<string, string>();
                userMetadata[MetadataConstants.TypeMetadataKey] = typeof(T).FullName;

                return SaveData(ms, location, userMetadata, idGenerator, options);
            }
        }

        public async Task<StoreLocation> SaveData(Stream data, StoreLocation location, IDictionary<string, string> userMetadata = null, IUniqueIdGenerator idGenerator = null, SecureStoreOptions options = SecureStoreOptions.All)
        {
            var metadata = new Dictionary<string, string>();

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

                data = _compressor.Compress(data);
                metadata.Add(MetadataConstants.CompressionMetadataKey, _compressor.Algorithm);
            }

            // Next is encryption
            if(options.HasFlag(SecureStoreOptions.Encrypt))
            {
                if(_encryptor == null)
                {
                    throw new ArgumentException("Encrypt option should not be used if no encryptor has been implemented", "options");
                }

                data = _encryptor.Encrypt(data);
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
            if(userMetadata != null)
            {
                foreach(var m in userMetadata)
                {
                    metadata[m.Key] = m.Value;
                }
            }

            /****************************************************
             *  SAVE THE INITIAL DATA
             * ***************************************************/
            await _store.SaveData(data, location, metadata);

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

        private string GetMessageDetails(StoreLocation location, IDictionary<string, string> metadata)
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
