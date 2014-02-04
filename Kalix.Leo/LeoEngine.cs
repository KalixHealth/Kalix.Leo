using AsyncBridge;
using Kalix.Leo.Configuration;
using Kalix.Leo.Internal;
using Kalix.Leo.Listeners;
using System;
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
            _indexListener = config.IndexQueue != null ? new IndexListener(config.IndexQueue, config.TypeResolver) : null;
            _cache = MemoryCache.Default;
            _cachePolicy = new CacheItemPolicy
            {
                Priority = CacheItemPriority.Default,
                SlidingExpiration = TimeSpan.FromHours(1)
            };

            _baseName = "LeoEngine::" + config.UniqueName + "::";

            if (!string.IsNullOrEmpty(config.KeyContainer))
            {
                using (var w = AsyncHelper.Wait)
                {
                    w.Run(config.BaseStore.CreateContainerIfNotExists(config.KeyContainer));

                    if (config.IndexStore != null)
                    {
                        w.Run(config.IndexStore.CreateContainerIfNotExists(config.KeyContainer));
                    }
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
                    _indexListener.RegisterTypeIndexer(obj.Type, new ItemPartitionIndexer(this, obj.Indexer, c => GetPartitionByType(obj.Type, c).Indexer));
                }

                foreach (var obj in config.Objects.Where(o => o.Type == null && o.Indexer != null))
                {
                    _indexListener.RegisterPathIndexer(obj.BasePath, new ItemPartitionIndexer(this, obj.Indexer, c => GetDocumentPartition(obj.BasePath, c).Indexer));
                }
            }
        }

        public IObjectPartition<T> GetObjectPartition<T>(string container)
            where T : ObjectWithId
        {
            var config = _config.Objects.FirstOrDefault(o => o.Type == typeof(T));
            if(config == null)
            {
                throw new InvalidOperationException("The object type '" + typeof(T).FullName + "' is not registered");
            }
            var key = _baseName + config.BasePath + "::" + container;
            var cacheVal = _cache.Get(key) as ObjectPartition<T>;
            if (cacheVal == null)
            {
                cacheVal = new ObjectPartition<T>(_config, container, config);
                _cache.Set(key, cacheVal, _cachePolicy); 
            }
            return cacheVal;
        }

        public IDocumentPartition GetDocumentPartition(string basePath, string container)
        {
            var config = _config.Objects.FirstOrDefault(o => o.Type == null && o.BasePath == basePath);
            if (config == null)
            {
                throw new InvalidOperationException("The document type with base path '" + basePath + "' is not registered");
            }

            var key = _baseName + config.BasePath + "::" + container;
            var cacheVal = _cache.Get(key) as DocumentPartition;
            if (cacheVal == null)
            {
                cacheVal = new DocumentPartition(_config, container, config);
                _cache.Set(key, cacheVal, _cachePolicy);
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

            if(_indexListener != null)
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
