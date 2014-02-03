using Kalix.Leo.Configuration;
using Kalix.Leo.Encryption;
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
        protected readonly Lazy<IEncryptor> _encryptor;

        protected SecureStoreOptions _options;

        public BasePartition(LeoEngineConfiguration engineConfig, string container, ItemConfiguration config)
        {
            _store = new SecureStore(engineConfig.BaseStore, engineConfig.BackupQueue, engineConfig.IndexQueue, engineConfig.Compressor);
            _container = container;
            _config = config;

            _options = SecureStoreOptions.KeepDeletes;
            if (config.DoBackup) { _options = _options | SecureStoreOptions.Backup; }
            if (config.Indexer != null) { _options = _options | SecureStoreOptions.Index; }
            if (config.DoCompress) { _options = _options | SecureStoreOptions.Compress; }

            _encryptor = new Lazy<IEncryptor>(() => config.DoEncrypt ? new CertProtectedEncryptor(engineConfig.BaseStore, new StoreLocation(engineConfig.KeyContainer, container), engineConfig.RsaCert) : null);
            _index = new Lazy<LuceneIndex>(() => engineConfig.IndexStore == null ? null : new LuceneIndex(new SecureStore(engineConfig.IndexStore, null, null, engineConfig.Compressor), container, config.BasePath, _encryptor.Value));
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
