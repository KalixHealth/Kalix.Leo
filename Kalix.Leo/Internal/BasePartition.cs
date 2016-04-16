using Kalix.Leo.Configuration;
using Kalix.Leo.Encryption;
using Kalix.Leo.Indexing;
using Kalix.Leo.Lucene;
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

        private readonly Lazy<LuceneIndex> _luceneIndex;
        private bool _isInit;
        private bool _disposed;

        public BasePartition(LeoEngineConfiguration engineConfig, long partitionId, ItemConfiguration config, Func<Task<IEncryptor>> encryptorFactory)
        {
            _store = new SecureStore(engineConfig.BaseStore, engineConfig.BackupQueue, engineConfig.IndexQueue, engineConfig.Compressor);
            _partitionId = partitionId;
            _config = config;

            _options = SecureStoreOptions.KeepDeletes;
            if (config.DoBackup) { _options = _options | SecureStoreOptions.Backup; }
            if (config.Indexer != null) { _options = _options | SecureStoreOptions.Index; }
            if (config.DoCompress) { _options = _options | SecureStoreOptions.Compress; }

            _encryptor = new Lazy<Task<IEncryptor>>(async () => config.DoEncrypt ? await encryptorFactory().ConfigureAwait(false) : null);

            string container = partitionId.ToString(CultureInfo.InvariantCulture);
            _luceneIndex = new Lazy<LuceneIndex>(() => engineConfig.IndexStore == null ? null : new LuceneIndex(new SecureStore(engineConfig.IndexStore, null, null, engineConfig.Compressor), container, config.BasePath, _encryptor, 20, 10));
        }

        public ISearchIndex<TMain, TSearch> Index<TMain, TSearch>(IRecordSearchComposition<TMain, TSearch> composition)
        {
            return new SearchIndex<TMain, TSearch>(composition, _encryptor, _partitionId);
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

        protected virtual async Task Initialise()
        {
            if (!_isInit)
            {
                _isInit = true;
                string container = _partitionId.ToString(CultureInfo.InvariantCulture);
                await _engineConfig.BaseStore.CreateContainerIfNotExists(container).ConfigureAwait(false);
            }
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
