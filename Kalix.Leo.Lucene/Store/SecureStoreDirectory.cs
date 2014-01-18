using AsyncBridge;
using Kalix.Leo.Storage;
using Lucene.Net.Store;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Path = System.IO.Path;

namespace Kalix.Leo.Lucene.Store
{
    public class SecureStoreDirectory : Directory
    {
        private readonly ISecureStore _store;
        private readonly string _container;
        private readonly IFileCache _cache;
        private readonly SecureStoreOptions _options;
        private readonly CompositeDisposable _disposables;

        public SecureStoreDirectory(ISecureStore store, string container, IFileCache cache)
        {
            _container = container;
            _cache = cache;
            _store = store;
            _disposables = new CompositeDisposable();

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
                w.Run(_cache.Delete(GetCachePath(name)));
            }
        }

        public override bool FileExists(string name)
        {
            // Always checks the server
            return GetSyncVal(_store.GetMetadata(GetLocation(name))) != null;
        }

        public override long FileLength(string name)
        {
            // Always checks the server
            var metadata = GetSyncVal(_store.GetMetadata(GetLocation(name)));
            return metadata == null || !metadata.Size.HasValue ? 0 : metadata.Size.Value;
        }

        public override long FileModified(string name)
        {
            // Always checks the server
            var metadata = GetSyncVal(_store.GetMetadata(GetLocation(name)));
            return metadata == null || !metadata.LastModified.HasValue ? 0 : metadata.LastModified.Value.ToFileTimeUtc();
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
            // Not sure this is used... (would be a bitch if it is!)
            throw new NotImplementedException();
        }

        public override IndexInput OpenInput(string name)
        {
            var input = new SecureStoreIndexInput(_cache, _store, GetLocation(name), GetCachePath(name), _disposables);
            _disposables.Add(input);
            return input;
        }

        public override IndexOutput CreateOutput(string name)
        {
            var loc = GetLocation(name);
            var output = new SecureStoreIndexOutput(_cache, GetCachePath(name), async data => 
            {
                // Use the original store metadata except for size/modified
                var metadata = await _store.GetMetadata(loc) ?? new Metadata();
                metadata.Size = data.Metadata.Size;
                metadata.LastModified = data.Metadata.LastModified;

                await _store.SaveData(loc, new DataWithMetadata(data.Stream, metadata, () => data.Dispose()), null, _options);
            });

            _disposables.Add(output);
            return output;
        }

        private Dictionary<string, SecureStoreLock> _locks = new Dictionary<string, SecureStoreLock>();
        public override Lock MakeLock(string name)
        {
            lock (_locks)
            {
                if (!_locks.ContainsKey(name))
                {
                    _locks.Add(name, new SecureStoreLock(_store, GetLocation(name)));
                }

                return _locks[name];
            }
        }

        public override void ClearLock(string name)
        {
            lock (_locks)
            {
                if (_locks.ContainsKey(name))
                {
                    _locks[name].Release();
                }
            }
        }

        public override string GetLockId()
        {
            return _container;
        }

        private StoreLocation GetLocation(string name)
        {
            return new StoreLocation(_container, name);
        }

        private string GetCachePath(string name)
        {
            return _container + Path.DirectorySeparatorChar + name;
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposables.IsDisposed)
            {
                foreach(var l in _locks.Values)
                {
                    l.Dispose();
                }

                _disposables.Dispose();
                _cache.Dispose();
            }
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
