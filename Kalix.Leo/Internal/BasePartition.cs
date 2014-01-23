using Kalix.Leo.Configuration;
using Kalix.Leo.Lucene;
using Kalix.Leo.Storage;
using System;

namespace Kalix.Leo.Internal
{
    public class BasePartition : IBasePartition, IDisposable
    {
        protected readonly ISecureStore _store;
        protected readonly string _container;
        protected readonly ItemConfiguration _config;
        protected readonly Lazy<LuceneIndex> _index;

        protected SecureStoreOptions _options;

        public BasePartition(LeoEngineConfiguration engineConfig, string container, ItemConfiguration config)
        {
            _store = engineConfig.SecureStore;
            _container = container;
            _config = config;

            _options = SecureStoreOptions.KeepDeletes;
            if (config.DoBackup) { _options = _options | SecureStoreOptions.Backup; }
            if (config.Indexer != null) { _options = _options | SecureStoreOptions.Index; }
            if (_store.CanCompress) { _options = _options | SecureStoreOptions.Compress; }
            if (_store.CanEncrypt) { _options = _options | SecureStoreOptions.Encrypt; }

            _index = new Lazy<LuceneIndex>(() => engineConfig.IndexStore == null ? null : new LuceneIndex(engineConfig.IndexStore, container, config.BasePath));
        }

        public ILuceneIndex Indexer
        {
            get { return _index.Value; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if(disposing)
            {
                if (_index.IsValueCreated)
                {
                    _index.Value.Dispose();
                }
            }
        }
    }
}
