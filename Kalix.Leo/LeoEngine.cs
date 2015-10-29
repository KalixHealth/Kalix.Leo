using Kalix.Leo.Configuration;
using Kalix.Leo.Indexing;
using Kalix.Leo.Listeners;
using System;
using System.Globalization;
using System.Linq;
using System.Reactive.Disposables;
using System.Reflection;
using System.Runtime.Caching;

namespace Kalix.Leo
{
    public class LeoEngine : IDisposable, ILeoEngine
    {
        private readonly LeoEngineConfiguration _config;
        private readonly IBackupListener _backupListener;
        private readonly IIndexListener _indexListener;
        private readonly Lazy<IRecordSearchComposer> _composer;
        
        private static readonly object _cacheLock = new object();
        private readonly MemoryCache _cache;
        private readonly CacheItemPolicy _cachePolicy;
        private readonly string _baseName;

        private bool _listenersStarted;
        private CompositeDisposable _disposables;

        public LeoEngine(LeoEngineConfiguration config)
        {
            _config = config;
            _disposables = new CompositeDisposable();
            _backupListener = config.BackupStore != null && config.BackupQueue != null ? new BackupListener(config.BackupQueue, config.BaseStore, config.BackupStore) : null;
            _indexListener = config.IndexQueue != null ? new IndexListener(config.IndexQueue, config.TypeResolver, config.TypeNameResolver) : null;
            _cache = MemoryCache.Default;
            _cachePolicy = new CacheItemPolicy
            {
                Priority = CacheItemPriority.Default,
                SlidingExpiration = TimeSpan.FromHours(1),
                RemovedCallback = (a) => 
                {
                    var disp = a.CacheItem.Value as IDisposable;
                    if(disp != null)
                    {
                        disp.Dispose();
                    }
                }
            };

            _baseName = "LeoEngine::" + config.UniqueName + "::";
            _composer = new Lazy<IRecordSearchComposer>(() => config.TableStore == null ? null : new RecordSearchComposer(config.TableStore));

            if (!string.IsNullOrEmpty(config.KeyContainer))
            {
                try
                {
                    config.BaseStore.CreateContainerIfNotExists(config.KeyContainer).Wait();
                }
                catch(AggregateException ex)
                {
                    throw ex.InnerException;
                }
            }

            if (_indexListener != null)
            {
                if (config.Objects == null)
                {
                    throw new ArgumentNullException("You have not initialised any objects");
                }

                if (config.Objects.Select(o => o.BasePath).Distinct().Count() != config.Objects.Count())
                {
                    throw new ArgumentException("Must have unique base paths accross all objects");
                }

                foreach (var obj in config.Objects.Where(o => o.Type != null && o.Indexer != null))
                {
                    _indexListener.RegisterTypeIndexer(obj.Type, obj.Indexer);
                }

                foreach (var obj in config.Objects.Where(o => o.Type == null && o.Indexer != null))
                {
                    _indexListener.RegisterPathIndexer(obj.BasePath, obj.Indexer);
                }
            }
        }

        public IRecordSearchComposer Composer
        {
            get { return _composer.Value; }
        }

        public IObjectPartition<T> GetObjectPartition<T>(long partitionId)
        {
            var config = _config.Objects.FirstOrDefault(o => o.Type == typeof(T));
            if(config == null)
            {
                throw new InvalidOperationException("The object type '" + typeof(T).FullName + "' is not registered");
            }
            var key = _baseName + config.BasePath + "::" + partitionId.ToString(CultureInfo.InvariantCulture);

            ObjectPartition<T> cacheVal;
            lock (_cacheLock)
            {
                cacheVal = _cache.Get(key) as ObjectPartition<T>;
                if(cacheVal == null)
                {
                    cacheVal = new ObjectPartition<T>(_config, partitionId, config);
                    _cache.Set(key, cacheVal, _cachePolicy);
                }
            }
            return cacheVal;
        }

        public IDocumentPartition GetDocumentPartition(string basePath, long partitionId)
        {
            var config = _config.Objects.FirstOrDefault(o => o.Type == null && o.BasePath == basePath);
            if (config == null)
            {
                throw new InvalidOperationException("The document type with base path '" + basePath + "' is not registered");
            }

            var key = _baseName + config.BasePath + "::" + partitionId.ToString(CultureInfo.InvariantCulture);

            DocumentPartition cacheVal;
            lock (_cacheLock)
            {
                cacheVal = _cache.Get(key) as DocumentPartition;
                if (cacheVal == null)
                {
                    cacheVal = new DocumentPartition(_config, partitionId, config);
                    _cache.Set(key, cacheVal, _cachePolicy);
                }
            }

            return cacheVal;
        }

        public void StartListeners(int? messagesToProcessInParallel = null)
        {
            if (_listenersStarted)
            {
                throw new InvalidOperationException("Listeners have already started");
            }

            if(_backupListener != null)
            {
                _disposables.Add(_backupListener.StartListener(_config.UncaughtExceptions, messagesToProcessInParallel));
            }

            if (_indexListener != null)
            {
                _disposables.Add(_indexListener.StartListener(_config.UncaughtExceptions, messagesToProcessInParallel));
            }

            _listenersStarted = true;
        }

        public void Dispose()
        {
            _disposables.Dispose();
        }

        private static MethodInfo _genericGetPartitionInfo = typeof(LeoEngine).GetMethod("GetObjectPartition");
        private IBasePartition GetPartitionByType(Type type, string container)
        {
            var method = _genericGetPartitionInfo.MakeGenericMethod(type);
            return (IBasePartition)method.Invoke(this, new[] { container });
        }
    }
}
