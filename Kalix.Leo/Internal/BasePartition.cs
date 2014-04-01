using AsyncBridge;
using Kalix.Leo.Configuration;
using Kalix.Leo.Encryption;
using Kalix.Leo.Indexing;
using Kalix.Leo.Lucene;
using Kalix.Leo.Storage;
using System;
using System.Globalization;

namespace Kalix.Leo.Internal
{
    public class BasePartition : IBasePartition, IDisposable
    {
        protected readonly ISecureStore _store;
        protected readonly long _partitionId;
        protected readonly ItemConfiguration _config;
        protected readonly Lazy<IEncryptor> _encryptor;
        protected readonly SecureStoreOptions _options;

        private readonly Lazy<LuceneIndex> _luceneIndex;
        private bool _disposed;

        public BasePartition(LeoEngineConfiguration engineConfig, long partitionId, ItemConfiguration config)
        {
            string container = partitionId.ToString(CultureInfo.InvariantCulture);
            using (var w = AsyncHelper.Wait)
            {
                w.Run(engineConfig.BaseStore.CreateContainerIfNotExists(container));
            }
            _store = new SecureStore(engineConfig.BaseStore, engineConfig.BackupQueue, engineConfig.IndexQueue, engineConfig.Compressor);
            _partitionId = partitionId;
            _config = config;

            _options = SecureStoreOptions.KeepDeletes;
            if (config.DoBackup) { _options = _options | SecureStoreOptions.Backup; }
            if (config.Indexer != null) { _options = _options | SecureStoreOptions.Index; }
            if (config.DoCompress) { _options = _options | SecureStoreOptions.Compress; }

            _encryptor = new Lazy<IEncryptor>(() => config.DoEncrypt ? new CertProtectedEncryptor(engineConfig.BaseStore, new StoreLocation(engineConfig.KeyContainer, container), engineConfig.RsaCert) : null);
            _luceneIndex = new Lazy<LuceneIndex>(() => engineConfig.IndexStore == null ? null : new LuceneIndex(new SecureStore(engineConfig.IndexStore, null, null, engineConfig.Compressor), container, config.BasePath, _encryptor.Value, 20, 10));
        }

        public ISearchIndex<TMain, TSearch> Index<TMain, TSearch>(IRecordSearchComposition<TMain, TSearch> composition)
        {
            return new SearchIndex<TMain, TSearch>(composition, _encryptor.Value, _partitionId);
        }


        public ILuceneIndex LuceneIndex
        {
            get { return _luceneIndex.Value; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_luceneIndex.IsValueCreated)
                    {
                        _luceneIndex.Value.Dispose();
                    }
                }

                _disposed = true;
            }
        }
    }
}
