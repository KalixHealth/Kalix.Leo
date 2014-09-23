using Kalix.Leo.Configuration;
using Kalix.Leo.Internal;
using Kalix.Leo.Storage;
using System;
using System.Globalization;
using System.IO;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class DocumentPartition : BasePartition, IDocumentPartition
    {
        public DocumentPartition(LeoEngineConfiguration engineConfig, long partitionId, ItemConfiguration config)
            : base(engineConfig, partitionId, config)
        {
        }

        public Task Save(string path, Func<Stream, Task> savingFunc, Metadata metadata = null)
        {
            return _store.SaveData(GetLocation(path), metadata, savingFunc, _encryptor.Value, _options);
        }

        public Task SaveMetadata(string path, Metadata metadata)
        {
            return _store.SaveMetadata(GetLocation(path), metadata, _options);
        }

        public Task<DataWithMetadata> Load(string path, string snapshot = null)
        {
            return _store.LoadData(GetLocation(path), snapshot, _encryptor.Value);
        }

        public Task<Metadata> GetMetadata(string path, string snapshot = null)
        {
            return _store.GetMetadata(GetLocation(path), snapshot);
        }

        public IObservable<Snapshot> FindSnapshots(string path)
        {
            return _store.FindSnapshots(GetLocation(path));
        }

        public IObservable<PathWithMetadata> Find(string prefix = null)
        {
            var baseLength = string.IsNullOrEmpty(_config.BasePath) ? 0 : _config.BasePath.Length + 1;
            string basePath = prefix == null ? _config.BasePath + "/" : Path.Combine(_config.BasePath, prefix);

            return _store.FindFiles(_partitionId.ToString(CultureInfo.InvariantCulture), basePath)
                .Select(l => new PathWithMetadata(l.Location.BasePath.Substring(baseLength), l.Metadata));
        }

        public Task Delete(string path)
        {
            return _store.Delete(GetLocation(path), _options);
        }

        public Task DeletePermanent(string path)
        {
            // Remove the keep deletes option...
            return _store.Delete(GetLocation(path), _options & ~SecureStoreOptions.KeepDeletes);
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
