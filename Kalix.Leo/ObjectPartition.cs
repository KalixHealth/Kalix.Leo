using Kalix.Leo.Configuration;
using Kalix.Leo.Internal;
using Kalix.Leo.Storage;
using System;
using System.Globalization;
using System.IO;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class ObjectPartition<T> : BasePartition, IObjectPartition<T>
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

        public Task Save(T data, long id, Metadata metadata = null)
        {
            var obj = new ObjectWithMetadata<T>(data, metadata);
            return _store.SaveObject(GetLocation(id), obj, _encryptor.Value, _options);
        }

        public Task SaveMetadata(long id, Metadata metadata)
        {
            return _store.SaveMetadata(GetLocation(id), metadata, _options);
        }

        public async Task<long> Save(T data, Expression<Func<T, long?>> idField, Action<long> preSaveProcessing = null, Metadata metadata = null)
        {
            var member = idField.Body as MemberExpression;
            if(member == null)
            {
                throw new ArgumentException(string.Format("Expression '{0}' refers to a method, not a property.", idField.ToString()));
            }

            var propInfo = member.Member as PropertyInfo;
            if(propInfo == null)
            {
                throw new ArgumentException(string.Format("Expression '{0}' refers to a field, not a property.", idField.ToString()));
            }

            var id = (long?)propInfo.GetValue(data);
            if (!id.HasValue)
            {
                id = await _idGenerator.Value.NextId();
                propInfo.SetValue(data, id);
            }

            if (preSaveProcessing != null)
            {
                preSaveProcessing(id.Value);
            }

            var obj = new ObjectWithMetadata<T>(data, metadata);
            await _store.SaveObject(GetLocation(id.Value), obj, _encryptor.Value, _options).ConfigureAwait(false);
            return id.Value;
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
            return _store.FindFiles(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath + "/")
                .Where(l => l.Location.Id.HasValue)
                .Select(l => new IdWithMetadata(l.Location.Id.Value, l.Metadata));
        }

        public Task Delete(long id)
        {
            return _store.Delete(GetLocation(id), _options);
        }

        public Task DeletePermanent(long id)
        {
            // Remove the keep deletes option...
            return _store.Delete(GetLocation(id), _options & ~SecureStoreOptions.KeepDeletes);
        }

        public Task ReIndexAll()
        {
            return _store.ReIndexAll(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath + "/");
        }

        public Task ReBackupAll()
        {
            return _store.BackupAll(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath + "/");
        }

        public Task SetInternalIdGenerator(long newId)
        {
            return _idGenerator.Value.SetCurrentId(newId);
        }

        private StoreLocation GetLocation(long? id)
        {
            return new StoreLocation(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath, id);
        }
    }
}
