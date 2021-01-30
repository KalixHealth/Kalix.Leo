using Kalix.Leo.Configuration;
using Kalix.Leo.Encryption;
using Kalix.Leo.Internal;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class DocumentPartition : BasePartition, IDocumentPartition
    {
        public DocumentPartition(LeoEngineConfiguration engineConfig, long partitionId, ItemConfiguration config, Func<Task<IEncryptor>> encFactory)
            : base(engineConfig, partitionId, config, encFactory)
        {
        }

        public async Task<Metadata> Save(string path, Func<IWriteAsyncStream, Task> savingFunc, UpdateAuditInfo audit, CancellationToken token, Metadata metadata = null)
        {
            await Initialise();
            var enc = await _encryptor.Value;
            return await _store.SaveData(GetLocation(path), metadata, audit, savingFunc, token, enc, _options);
        }

        public async Task<Metadata> SaveMetadata(string path, Metadata metadata)
        {
            await Initialise();
            return await _store.SaveMetadata(GetLocation(path), metadata, _options);
        }

        public async Task<DataWithMetadata> Load(string path, string snapshot = null)
        {
            await Initialise();
            var enc = await _encryptor.Value;
            return await _store.LoadData(GetLocation(path), snapshot, enc);
        }

        public async Task<Metadata> GetMetadata(string path, string snapshot = null)
        {
            await Initialise();
            return await _store.GetMetadata(GetLocation(path), snapshot);
        }

        public IAsyncEnumerable<Snapshot> FindSnapshots(string path)
        {
            return _store.FindSnapshots(GetLocation(path));
        }

        public IAsyncEnumerable<PathWithMetadata> Find(string prefix = null)
        {
            var baseLength = string.IsNullOrEmpty(_config.BasePath) ? 0 : _config.BasePath.Length + 1;
            string basePath = prefix == null ? _config.BasePath + "/" : Path.Combine(_config.BasePath, prefix);

            return _store.FindFiles(_partitionId.ToString(CultureInfo.InvariantCulture), basePath)
                .Select(l => new PathWithMetadata(l.Location.BasePath.Substring(baseLength), l.Metadata));
        }

        public async Task Delete(string path, UpdateAuditInfo audit)
        {
            await Initialise();
            await _store.Delete(GetLocation(path), audit, _options);
        }

        public async Task DeletePermanent(string path)
        {
            await Initialise();
            // Remove the keep deletes option...
            await _store.Delete(GetLocation(path), null, _options & ~SecureStoreOptions.KeepDeletes);
        }

        public Task ForceIndex(string path, Metadata metadata = null)
        {
            return _store.ForceIndex(GetLocation(path), metadata);
        }

        public Task ReIndexAll()
        {
            // All documents in the index can be reindexed...
            return _store.ReIndexAll(_partitionId.ToString(CultureInfo.InvariantCulture), l => true, _config.BasePath + "/");
        }

        public Task ReBackupAll()
        {
            return _store.BackupAll(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath + "/");
        }

        private StoreLocation GetLocation(string path)
        {
            return new StoreLocation(_partitionId.ToString(CultureInfo.InvariantCulture), Path.Combine(_config.BasePath, path));
        }
    }
}
