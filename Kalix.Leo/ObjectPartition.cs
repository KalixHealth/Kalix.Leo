using Kalix.Leo.Configuration;
using Kalix.Leo.Internal;
using Kalix.Leo.Storage;
using System;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class ObjectPartition<T> : BasePartition, IObjectPartition<T>
    {
        protected readonly Lazy<UniqueIdGenerator> _idGenerator;

        public ObjectPartition(LeoEngineConfiguration engineConfig, string container, ItemConfiguration config)
            : base(engineConfig, container, config)
        {
            _options = _options | SecureStoreOptions.GenerateId;

            _idGenerator = new Lazy<UniqueIdGenerator>(() =>
            {
                var loc = new StoreLocation(container, engineConfig.UniqueIdGeneratorPath);
                return new UniqueIdGenerator(engineConfig.BaseStore, loc, 5);
            });
        }

        public async Task<long> Save(long? id, T data, IMetadata metadata = null)
        {
            var obj = new ObjectWithMetadata<T>(data, metadata);
            var result = await _store.SaveObject(GetLocation(id), obj, _idGenerator.Value, _options);
            return result.Id.Value;
        }

        public Task<ObjectWithMetadata<T>> Load(long id, string snapshot = null)
        {
            return _store.LoadObject<T>(GetLocation(id), snapshot);
        }

        public Task<IMetadata> GetMetadata(long id, string snapshot = null)
        {
            return _store.GetMetadata(GetLocation(id), snapshot);
        }

        public IObservable<Snapshot> FindSnapshots(long id)
        {
            return _store.FindSnapshots(GetLocation(id));
        }

        public IObservable<IdWithMetadata> FindAll()
        {
            return _store.FindFiles(_container, _config.BasePath)
                .Where(l => l.Location.Id.HasValue)
                .Select(l => new IdWithMetadata(l.Location.Id.Value, l.Metadata));
        }

        public Task Delete(long id)
        {
            return _store.Delete(GetLocation(id), _options);
        }

        public Task ReIndexAll()
        {
            return _store.ReIndexAll(_container, _config.BasePath);
        }

        public Task ReBackupAll()
        {
            return _store.BackupAll(_container, _config.BasePath);
        }

        private StoreLocation GetLocation(long? id)
        {
            return new StoreLocation(_container, _config.BasePath, id);
        }
    }
}
