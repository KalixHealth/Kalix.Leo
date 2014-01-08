using AsyncBridge;
using Kalix.Leo.Caching;
using Lucene.Net.Store;
using System;
using System.Threading.Tasks;
using System.Reactive.Linq;
using System.Linq;

namespace Kalix.Leo.Lucene
{
    public class SecureStoreDirectory : Directory
    {
        private readonly ISecureStore _store;
        private readonly string _container;
        private readonly ICache _cache;
        private readonly SecureStoreOptions _options;

        private readonly string _storeCachePrefix;
        private readonly string _lengthCachePrefix;
        private readonly string _modifiedCachePrefix;

        public SecureStoreDirectory(ISecureStore store, string container, ICache cache)
        {
            _container = container;
            _cache = cache;
            _store = store;
            _storeCachePrefix = "SSD::S::" + _container + "::";
            _lengthCachePrefix = "SSD::L::" + _container + "::";
            _modifiedCachePrefix = "SSD::M::" + _container + "::";

            _options = SecureStoreOptions.None;
            if (_store.CanEncrypt)
            {
                _options = SecureStoreOptions.Encrypt;
            }
            if (_store.CanCompress)
            {
                _options = _options | SecureStoreOptions.Compress;
            }
        }

        public override void DeleteFile(string name)
        {
            using (var w = AsyncHelper.Wait)
            {
                w.Run(_store.Delete(GetLocation(name), SecureStoreOptions.None));
                w.Run(_cache.Clear(_storeCachePrefix + name));
            }
        }

        public override bool FileExists(string name)
        {
            bool exists = false;
            var t1 = _cache.Get<object>(_storeCachePrefix + name);

            if (!t1.IsCompleted)
            {
                using (var w = AsyncHelper.Wait)
                {
                    var t2 = _store.GetMetadata(GetLocation(name)).ContinueWith(t => (object)t.Result);
                    w.Run(Task.WhenAny(t1, t2).ContinueWith(t => { exists = t.Result != null; }));
                }
            }
            else
            {
                exists = t1.Result != null;
            }

            return exists;
        }

        public override long FileLength(string name)
        {
            var task = _cache.Get<long>(_lengthCachePrefix + name, async () =>
            {
                var m = await _store.GetMetadata(GetLocation(name)).ConfigureAwait(false);
                long length = 0;
                if (m != null) 
                {
                    long.TryParse(m[MetadataConstants.SizeMetadataKey], out length);
                }
                return length;
            });

            return GetSyncVal(task);
        }

        public override long FileModified(string name)
        {
            var task = _cache.Get<long>(_modifiedCachePrefix + name, async () =>
            {
                var m = await _store.GetMetadata(GetLocation(name)).ConfigureAwait(false);
                long modified = 0;
                if (m != null)
                {
                    long inner;
                    if (long.TryParse(m[MetadataConstants.ModifiedMetadataKey], out inner))
                    {
                        var date = new DateTime(inner, DateTimeKind.Utc);
                        modified = date.ToFileTimeUtc();
                    }
                }
                return modified;
            });

            return GetSyncVal(task);
        }

        public override string[] ListAll()
        {
            return _store
                .FindFiles(_container)
                .Select(s => s.BasePath)
                .ToEnumerable()
                .ToArray(); // This will block until executed
        }

        public override void TouchFile(string name)
        {
            // Do nothing...
        }

        public override IndexInput OpenInput(string name)
        {
            throw new NotImplementedException();
        }

        public override IndexOutput CreateOutput(string name)
        {
            throw new NotImplementedException();
        }

        private StoreLocation GetLocation(string path)
        {
            return new StoreLocation(_container, path);
        }

        protected override void Dispose(bool disposing)
        {
        }

        private T GetSyncVal<T>(Task<T> task)
        {
            T val;
            if (task.IsCompleted)
            {
                val = task.Result;
            }
            else
            {
                val = default(T);
                using (var w = AsyncHelper.Wait)
                {
                    w.Run(task.ContinueWith(t => { val = t.Result; }));
                }
            }
            return val;
        }
    }
}
