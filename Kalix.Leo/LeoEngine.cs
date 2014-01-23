using Kalix.Leo.Configuration;
using Kalix.Leo.Internal;
using Kalix.Leo.Listeners;
using System;
using System.Linq;
using System.Reactive.Disposables;
using System.Reflection;

namespace Kalix.Leo
{
    public class LeoEngine : IDisposable, ILeoEngine
    {
        private readonly LeoEngineConfiguration _config;
        private readonly IBackupListener _backupListener;
        private readonly IIndexListener _indexListener;

        private bool _listenersStarted;
        private CompositeDisposable _disposables;

        public LeoEngine(LeoEngineConfiguration config)
        {
            _config = config;
            _disposables = new CompositeDisposable();
            _backupListener = config.BackupStore != null && config.BackupQueue != null ? new BackupListener(config.BackupQueue, config.BaseStore, config.BackupStore) : null;
            _indexListener = config.IndexQueue != null ? new IndexListener(config.IndexQueue, config.TypeResolver) : null;

            if (_indexListener != null && config.Objects != null)
            {
                foreach (var obj in config.Objects.Where(o => o.Type != null && o.Indexer != null))
                {
                    _indexListener.RegisterTypeIndexer(obj.Type, new ItemPartitionIndexer(obj.Indexer, c => GetPartitionByType(obj.Type, c).Indexer));
                }

                foreach (var obj in config.Objects.Where(o => o.Type == null && o.Indexer != null))
                {
                    _indexListener.RegisterPathIndexer(obj.BasePath, new ItemPartitionIndexer(obj.Indexer, c => GetDocumentPartition(obj.BasePath, c).Indexer));
                }
            }
        }

        public IObjectPartition<T> GetObjectPartition<T>(string container)
        {
            var config = _config.Objects.FirstOrDefault(o => o.Type == typeof(T));
            if(config == null)
            {
                throw new InvalidOperationException("The object type '" + typeof(T).FullName + "' is not registered");
            }

            return new ObjectPartition<T>(_config, container, config);
        }

        public IDocumentPartition GetDocumentPartition(string basePath, string container)
        {
            var config = _config.Objects.FirstOrDefault(o => o.Type == null && o.BasePath == basePath);
            if (config == null)
            {
                throw new InvalidOperationException("The document type with base path '" + basePath + "' is not registered");
            }

            return new DocumentPartition(_config, container, config);
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
