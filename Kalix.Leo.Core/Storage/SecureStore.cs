using Kalix.Leo.Compression;
using Kalix.Leo.Encryption;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Storage
{
    public class SecureStore : ISecureStore
    {
        private readonly IOptimisticStore _store;
        private readonly ICompressor _compressor;

        public SecureStore(IOptimisticStore store, ICompressor compressor = null)
        {
            _store = store ?? throw new ArgumentNullException(nameof(store));
            _compressor = compressor;
        }

        public bool CanCompress
        {
            get { return _compressor != null; }
        }

        public IAsyncEnumerable<Snapshot> FindSnapshots(StoreLocation location)
        {
            return _store.FindSnapshots(location);
        }

        public IAsyncEnumerable<LocationWithMetadata> FindFiles(string container, string prefix = null)
        {
            return _store.FindFiles(container, prefix);
        }

        public async Task<ObjectWithMetadata<T>> LoadObject<T>(StoreLocation location, string snapshot = null, IEncryptor encryptor = null)
            where T : ObjectWithAuditInfo
        {
            var data = await LoadData(location, snapshot, encryptor);
            if(data == null) { return null; }
            
            if(!data.Metadata.ContainsKey(MetadataConstants.TypeMetadataKey))
            {
                LeoTrace.WriteLine(string.Format("Warning: Data type is not in metadata. expected {0}", typeof(T).FullName));
            }
            else if (data.Metadata[MetadataConstants.TypeMetadataKey] != typeof(T).FullName)
            {
                LeoTrace.WriteLine(string.Format("Warning: Data type does not match metadata. actual '{0}' vs expected '{1}'", data.Metadata[MetadataConstants.TypeMetadataKey], typeof(T).FullName));
            }

            LeoTrace.WriteLine("Getting data object: " + location);

            using var ms = new MemoryStream();
            await data.Reader.CopyToAsync(ms);
            var strData = ms.ToArray();
            var str = Encoding.UTF8.GetString(strData, 0, strData.Length);
            var obj = JsonConvert.DeserializeObject<T>(str);
            obj.Audit = data.Metadata.Audit;

            LeoTrace.WriteLine("Returning data object: " + location);
            return new ObjectWithMetadata<T>(obj, data.Metadata);
        }

        public async Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null, IEncryptor encryptor = null)
        {
            var data = await _store.LoadData(location, snapshot);
            if (data == null) { return null; }

            var metadata = data.Metadata;
            var stream = data.Reader;

            // Check encryption algorithm
            var hasEncryption = metadata.ContainsKey(MetadataConstants.EncryptionMetadataKey);
            if(hasEncryption && metadata[MetadataConstants.EncryptionMetadataKey] != encryptor.Algorithm)
            {
                throw new InvalidOperationException("Encryption Algorithms do not match, cannot load data");
            }

            // Check decompression algorithm
            var hasCompression = metadata.ContainsKey(MetadataConstants.CompressionMetadataKey);
            if (hasCompression && metadata[MetadataConstants.CompressionMetadataKey] != _compressor.Algorithm)
            {
                throw new InvalidOperationException("Compression Algorithms do not match, cannot load data");
            }

            // Modify read stream if required
            if (hasEncryption || hasCompression)
            {
                var s = stream.AsStream();

                // This is a read flow, so decryption comes first
                if (hasEncryption)
                {
                    s = encryptor.Decrypt(s, true);
                }

                // Then comes the decompression
                if (hasCompression)
                {
                    s = _compressor.DecompressReadStream(s);
                }

                stream = PipeReader.Create(s);
            }

            return new DataWithMetadata(stream, metadata);
        }

        public Task<Metadata> GetMetadata(StoreLocation location, string snapshot = null)
        {
            return _store.GetMetadata(location, snapshot);
        }

        public async Task<Metadata> SaveObject<T>(StoreLocation location, ObjectWithMetadata<T> obj, UpdateAuditInfo audit, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All)
            where T : ObjectWithAuditInfo
        {
            // Serialise to json as more cross platform
            obj.Data.HideAuditInfo = true;
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj.Data));
            obj.Data.HideAuditInfo = false;
            obj.Metadata[MetadataConstants.TypeMetadataKey] = typeof(T).FullName;

            var ct = CancellationToken.None;
            var metadata = await SaveData(location, obj.Metadata, audit, async s => await s.WriteAsync(data.AsMemory(), ct), ct, encryptor, options);
            obj.Data.Audit = metadata.Audit;
            return metadata;
        }

        public async Task<Metadata> SaveData(StoreLocation location, Metadata mdata, UpdateAuditInfo audit, Func<PipeWriter, ValueTask> savingFunc, CancellationToken token, IEncryptor encryptor = null, SecureStoreOptions options = SecureStoreOptions.All)
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
                    throw new ArgumentException("Compression option should not be used if no compressor has been implemented");
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
            var m = await _store.SaveData(location, metadata, audit, async (stream) =>
            {
                var s = stream.AsStream();
                // Encrypt just before writing to the stream (if we need)
                if (encryptor != null)
                {
                    s = encryptor.Encrypt(s, false);
                }

                // Compression comes right before encryption
                if (options.HasFlag(SecureStoreOptions.Compress))
                {
                    s = _compressor.CompressWriteStream(s);
                }

                // Always place the length counter stream
                var counter = new LengthCounterStream(s);

                var pipeWriter = PipeWriter.Create(counter);

                await savingFunc(pipeWriter);
                await pipeWriter.CompleteAsync();

                return counter.Length;
            }, token);

            return m;
        }

        public Task<Metadata> SaveMetadata(StoreLocation location, Metadata metadata, SecureStoreOptions options = SecureStoreOptions.All)
        {
            return _store.SaveMetadata(location, metadata);
        }

        public async Task Delete(StoreLocation location, UpdateAuditInfo audit, SecureStoreOptions options = SecureStoreOptions.All)
        {
            var metadata = await _store.GetMetadata(location);
            if (metadata == null) { return; }

            if (options.HasFlag(SecureStoreOptions.KeepDeletes))
            {
                await _store.SoftDelete(location, audit);
            }
            else
            {
                await _store.PermanentDelete(location);
            }
        }

        public Task<(IAsyncDisposable CancelDispose, Task Task)> Lock(StoreLocation location)
        {
            return _store.Lock(location);
        }

        public Task RunOnce(StoreLocation location, Func<Task> action)
        {
            return _store.RunOnce(location, action);
        }

        public IAsyncEnumerable<bool> RunEvery(StoreLocation location, TimeSpan interval, Action<Exception> unhandledExceptions = null)
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
    }
}
