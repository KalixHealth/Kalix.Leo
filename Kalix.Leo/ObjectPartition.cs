using Kalix.Leo.Configuration;
using Kalix.Leo.Encryption;
using Kalix.Leo.Internal;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class ObjectPartition<T> : BasePartition, IObjectPartition<T>
        where T : ObjectWithAuditInfo
    {
        private readonly Lazy<UniqueIdGenerator> _idGenerator;

        public ObjectPartition(LeoEngineConfiguration engineConfig, long partitionId, ItemConfiguration config, Func<Task<IEncryptor>> encFactory)
            : base(engineConfig, partitionId, config, encFactory)
        {
            _idGenerator = new Lazy<UniqueIdGenerator>(() =>
            {
                var loc = new StoreLocation(partitionId.ToString(CultureInfo.InvariantCulture), Path.Combine(config.BasePath, engineConfig.UniqueIdGeneratorPath));
                return new UniqueIdGenerator(engineConfig.BaseStore, loc, 5);
            }, true);
        }

        public async Task<ObjectPartitionWriteResult<T>> Save(T data, long id, UpdateAuditInfo audit, Metadata metadata = null)
        {
            await Initialise().ConfigureAwait(false);
            var enc = await _encryptor.Value.ConfigureAwait(false);
            var obj = new ObjectWithMetadata<T>(data, metadata);
            var result = await _store.SaveObject(GetLocation(id), obj, audit, enc, _options).ConfigureAwait(false);
            return new ObjectPartitionWriteResult<T>
            {
                Id = id,
                Data = new ObjectWithMetadata<T>(data, result)
            };
        }

        public async Task<Metadata> SaveMetadata(long id, Metadata metadata)
        {
            await Initialise().ConfigureAwait(false);
            return await _store.SaveMetadata(GetLocation(id), metadata, _options).ConfigureAwait(false);
        }

        public async Task<ObjectPartitionWriteResult<T>> Save(T data, Expression<Func<T, long?>> idField, UpdateAuditInfo audit, Func<long, Task> preSaveProcessing = null, Metadata metadata = null)
        {
            await Initialise().ConfigureAwait(false);

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
                id = await GetNextId().ConfigureAwait(false);
                propInfo.SetValue(data, id);
            }

            if (preSaveProcessing != null)
            {
                await preSaveProcessing(id.Value).ConfigureAwait(false);
            }

            var enc = await _encryptor.Value.ConfigureAwait(false);
            var obj = new ObjectWithMetadata<T>(data, metadata);
            var result = await _store.SaveObject(GetLocation(id.Value), obj, audit, enc, _options).ConfigureAwait(false);
            return new ObjectPartitionWriteResult<T>
            {
                Id = id.Value,
                Data = new ObjectWithMetadata<T>(data, result)
            };
        }

        public async Task<ObjectWithMetadata<T>> Load(long id, string snapshot = null)
        {
            await Initialise().ConfigureAwait(false);
            var enc = await _encryptor.Value.ConfigureAwait(false);
            return await _store.LoadObject<T>(GetLocation(id), snapshot, enc).ConfigureAwait(false);
        }

        public async Task<Metadata> GetMetadata(long id, string snapshot = null)
        {
            await Initialise().ConfigureAwait(false);
            return await _store.GetMetadata(GetLocation(id), snapshot).ConfigureAwait(false);
        }

        public IAsyncEnumerable<Snapshot> FindSnapshots(long id)
        {
            return _store.FindSnapshots(GetLocation(id));
        }

        public IAsyncEnumerable<IdWithMetadata> FindAll()
        {
            return _store.FindFiles(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath + "/")
                .Where(l => l.Location.Id.HasValue)
                .Select(l => new IdWithMetadata(l.Location.Id.Value, l.Metadata));
        }

        public async Task Delete(long id, UpdateAuditInfo audit)
        {
            await Initialise().ConfigureAwait(false);
            await _store.Delete(GetLocation(id), audit, _options).ConfigureAwait(false);
        }

        public async Task DeletePermanent(long id)
        {
            await Initialise().ConfigureAwait(false);
            // Remove the keep deletes option...
            await _store.Delete(GetLocation(id), null, _options & ~SecureStoreOptions.KeepDeletes).ConfigureAwait(false);
        }

        public Task ForceIndex(long id, Metadata metadata = null)
        {
            var loc = GetLocation(id);
            return _store.ForceObjectIndex<T>(loc, metadata);
        }

        public Task ReIndexAll()
        {
            // Only try to reindex values that actually have an id
            return _store.ReIndexAll(_partitionId.ToString(CultureInfo.InvariantCulture), f => f.Location.Id.HasValue, _config.BasePath + "/");
        }

        public Task ReBackupAll()
        {
            return _store.BackupAll(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath + "/");
        }

        public Task SetInternalIdGenerator(long newId)
        {
            return _idGenerator.Value.SetCurrentId(newId);
        }

        public Task<long> GetNextId()
        {
            return _idGenerator.Value.NextId();
        }

        private StoreLocation GetLocation(long? id)
        {
            return new StoreLocation(_partitionId.ToString(CultureInfo.InvariantCulture), _config.BasePath, id);
        }
    }
}
