using Kalix.Leo.Configuration;
using Kalix.Leo.Encryption;
using Kalix.Leo.Indexing;
using Kalix.Leo.Storage;
using System;
using System.Globalization;
using System.Threading.Tasks;

namespace Kalix.Leo.Internal
{
    public class BasePartition : IBasePartition, IDisposable
    {
        protected readonly ISecureStore _store;
        protected readonly long _partitionId;
        protected readonly ItemConfiguration _config;
        protected readonly Lazy<Task<IEncryptor>> _encryptor;
        protected readonly SecureStoreOptions _options;
        protected readonly LeoEngineConfiguration _engineConfig;

        private bool _isInit;
        private bool _disposed;

        public BasePartition(LeoEngineConfiguration engineConfig, long partitionId, ItemConfiguration config, Func<Task<IEncryptor>> encryptorFactory)
        {
            _store = new SecureStore(engineConfig.BaseStore, engineConfig.BackupQueue, engineConfig.IndexQueue, engineConfig.SecondaryIndexQueue, engineConfig.Compressor);
            _partitionId = partitionId;
            _config = config;
            _engineConfig = engineConfig;

            _options = SecureStoreOptions.KeepDeletes;
            if (config.DoBackup) { _options = _options | SecureStoreOptions.Backup; }
            if (config.Indexer != null) { _options = _options | SecureStoreOptions.Index; }
            if (config.DoCompress) { _options = _options | SecureStoreOptions.Compress; }

            _encryptor = new Lazy<Task<IEncryptor>>(async () => config.DoEncrypt ? await encryptorFactory() : null, true);
        }

        public ISearchIndex<TMain, TSearch> Index<TMain, TSearch>(IRecordSearchComposition<TMain, TSearch> composition)
        {
            return new SearchIndex<TMain, TSearch>(composition, _encryptor, _partitionId);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual async Task Initialise()
        {
            if (!_isInit)
            {
                _isInit = true;
                string container = _partitionId.ToString(CultureInfo.InvariantCulture);
                await _engineConfig.BaseStore.CreateContainerIfNotExists(container);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                // Nothing to dispose...

                _disposed = true;
            }
        }
    }
}
