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
    public class ObjectPartition<T> : BasePartition, IObjectPartition<T>
        where T : ObjectWithId
    {
        protected readonly Lazy<UniqueIdGenerator> _idGenerator;

        public ObjectPartition(LeoEngineConfiguration engineConfig, long partitionId, ItemConfiguration config)
            : base(engineConfig, partitionId, config)
        {
            _idGenerator = new Lazy<UniqueIdGenerator>(() =>
            {
                var loc = new StoreLocation(partitionId.ToString(CultureInfo.InvariantCulture), Path.Combine(config.BasePath, engineConfig.UniqueIdGeneratorPath));
                return new UniqueIdGenerator(engineConfig.BaseStore, loc, 5);
            });
        }

        public async Task<long> Save(T data, Metadata metadata = null)
        {
            if(!data.Id.HasValue)
            {
                data.Id = await _idGenerator.Value.NextId();
            }

            var obj = new ObjectWithMetadata<T>(data, metadata);
            await _store.SaveObject(GetLocation(data.Id.Value), obj, _encryptor.Value, _options);
            return data.Id.Value;
        }

        public Task<ObjectWithMetadata<T>> Load(long id, string snapshot = null)
        {
            return _store.LoadObject<T>(GetLocation(id), snapshot, _encryptor.Value);
        }

        public Task<Metadata> GetMetadata(long id, string snapshot = null)
        {
            return _store.GetMetadata(GetLocation(id), snapshot);
        }

        public IObservable<Snapshot> FindSnapshots(long id)
        {
            return _store.FindSnapshots(GetLocation(id));
        }

        public IObservable<IdWithMetadata> FindAll()
        {
            return _store.FindFiles(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath)
                .Where(l => l.Location.Id.HasValue)
                .Select(l => new IdWithMetadata(l.Location.Id.Value, l.Metadata));
        }

        public Task Delete(long id)
        {
            return _store.Delete(GetLocation(id), _options);
        }

        public Task ReIndexAll()
        {
            return _store.ReIndexAll(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath);
        }

        public Task ReBackupAll()
        {
            return _store.BackupAll(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath);
        }

        private StoreLocation GetLocation(long? id)
        {
            return new StoreLocation(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath, id);
        }
    }
}
