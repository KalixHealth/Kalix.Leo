using Kalix.Leo.Configuration;
using Kalix.Leo.Encryption;
using Kalix.Leo.Indexing;
using Kalix.Leo.Storage;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo;

public class LeoEngine : ILeoEngine
{
    private readonly LeoEngineConfiguration _config;
    private readonly Lazy<IRecordSearchComposer> _composer;
        
    private readonly IMemoryCache _cache;
    private readonly MemoryCacheEntryOptions _cachePolicy;
    private readonly string _baseName;

    private bool _hasInitEncryptorContainer;

    public LeoEngine(LeoEngineConfiguration config, IMemoryCache cache)
    {
        _config = config;
        _cache = cache;
        _cachePolicy = new MemoryCacheEntryOptions()
            .SetPriority(CacheItemPriority.NeverRemove)
            .SetSlidingExpiration(TimeSpan.FromHours(1))
            .RegisterPostEvictionCallback((key, value, reason, state) =>
            {
                if (value is IDisposable disp)
                {
                    disp.Dispose();
                }
            });

        _baseName = "LeoEngine::" + config.UniqueName + "::";
        _composer = new Lazy<IRecordSearchComposer>(() => config.TableStore == null ? null : new RecordSearchComposer(config.TableStore), true);
    }

    public IRecordSearchComposer Composer
    {
        get { return _composer.Value; }
    }

    public IObjectPartition<T> GetObjectPartition<T>(long partitionId)
        where T : ObjectWithAuditInfo
    {
        var config = _config.Objects.FirstOrDefault(o => o.Type == typeof(T));
        if(config == null)
        {
            throw new InvalidOperationException("The object type '" + typeof(T).FullName + "' is not registered");
        }
        var key = _baseName + config.BasePath + "::" + partitionId.ToString(CultureInfo.InvariantCulture);

        return GetCachedValue(key, () => new ObjectPartition<T>(_config, partitionId, config, () => GetEncryptor(partitionId)));
    }

    public IDocumentPartition GetDocumentPartition(string basePath, long partitionId)
    {
        var config = _config.Objects.FirstOrDefault(o => o.Type == null && o.BasePath == basePath);
        if (config == null)
        {
            throw new InvalidOperationException("The document type with base path '" + basePath + "' is not registered");
        }

        var key = _baseName + config.BasePath + "::" + partitionId.ToString(CultureInfo.InvariantCulture);

        return GetCachedValue(key, () => new DocumentPartition(_config, partitionId, config, () => GetEncryptor(partitionId)));
    }

    public Task<IEncryptor> GetEncryptor(long partitionId)
    {
        if (!_hasInitEncryptorContainer && !string.IsNullOrEmpty(_config.KeyContainer))
        {
            _config.BaseStore.CreateContainerIfNotExists(_config.KeyContainer);
            _hasInitEncryptorContainer = true;
        }

        var partitionKey = partitionId.ToString(CultureInfo.InvariantCulture);
        var key = _baseName + "Encryptor::" + partitionKey;
        return GetCachedValue(key, () => CertProtectedEncryptor.CreateEncryptor(_config.BaseStore, new StoreLocation(_config.KeyContainer, partitionKey), _config.RsaCert));
    }

    private Task<T> GetCachedValue<T>(string key, Func<Task<T>> factory)
        where T : class
    {
        return _cache.GetOrCreateAsync(key, async e =>
        {
            e.SetOptions(_cachePolicy);
            return await factory();
        });
    }

    private T GetCachedValue<T>(string key, Func<T> factory)
        where T : class, IDisposable
    {
        return _cache.GetOrCreate(key, e =>
        {
            e.SetOptions(_cachePolicy);
            return factory();
        });
    }
}