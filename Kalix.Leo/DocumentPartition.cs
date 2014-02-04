using Kalix.Leo.Configuration;
using Kalix.Leo.Internal;
using Kalix.Leo.Storage;
using System;
using System.IO;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class DocumentPartition : BasePartition, IDocumentPartition
    {
        public DocumentPartition(LeoEngineConfiguration engineConfig, string container, ItemConfiguration config)
            : base(engineConfig, container, config)
        {
        }

        public Task Save(string path, IObservable<byte[]> data, Metadata metadata = null)
        {
            var obj = new DataWithMetadata(data, metadata);
            return _store.SaveData(GetLocation(path), obj, _encryptor.Value, _options);
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

            return _store.FindFiles(_container, Path.Combine(_config.BasePath, prefix))
                .Select(l => new PathWithMetadata(l.Location.BasePath.Substring(baseLength), l.Metadata));
        }

        public Task Delete(string path)
        {
            return _store.Delete(GetLocation(path), _options);
        }

        public Task ReIndexAll()
        {
            return _store.ReIndexAll(_container, _config.BasePath);
        }

        public Task ReBackupAll()
        {
            return _store.BackupAll(_container, _config.BasePath);
        }

        private StoreLocation GetLocation(string path)
        {
            return new StoreLocation(_container, Path.Combine(_config.BasePath, path));
        }
    }
}
