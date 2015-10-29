using Kalix.Leo.Encryption;
using Kalix.Leo.Storage;
using Lucene.Net.Store;
using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using Path = System.IO.Path;

namespace Kalix.Leo.Lucene.Store
{
    public class SecureStoreDirectory : Directory
    {
        private readonly ISecureStore _store;
        private readonly string _container;
        private readonly string _basePath;
        private readonly IEncryptor _encryptor;
        private readonly SecureStoreOptions _options;
        private readonly Directory _cache;

        public SecureStoreDirectory(Directory cache, ISecureStore store, string container, string basePath, IEncryptor encryptor)
        {
            _container = container;
            _basePath = basePath ?? string.Empty;
            _cache = cache;
            _store = store;
            _encryptor = encryptor;

            _options = SecureStoreOptions.None;
            if (_store.CanCompress)
            {
                _options = _options | SecureStoreOptions.Compress;
            }

            store.CreateContainerIfNotExists(container).WaitAndWrap();
        }

        public void ClearCache()
        {
            foreach (string file in _cache.ListAll())
            {
                _cache.DeleteFile(file);
            }
        }

        /// <summary>Returns an array of strings, one for each file in the directory. </summary>
        public override string[] ListAll()
        {
            return ListAllAsync().ResultAndWrap();
        }

        private async Task<string[]> ListAllAsync()
        {
            int basePathLength = string.IsNullOrEmpty(_basePath) ? 0 : _basePath.Length + 1;

            return await _store
                .FindFiles(_container, string.IsNullOrEmpty(_basePath) ? null : (_basePath + '/'))
                .Select(s => s.Location.BasePath.Substring(basePathLength))
                .ToArray()
                .ToTask()
                .ConfigureAwait(false);
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        public override bool FileExists(string name)
        {
            var metadata = _store.GetMetadata(GetLocation(name)).ResultAndWrap();
            return metadata != null;
        }

        /// <summary>Returns the time the named file was last modified. </summary>
        public override long FileModified(string name)
        {
            var metadata = _store.GetMetadata(GetLocation(name)).ResultAndWrap();
            return metadata == null || !metadata.LastModified.HasValue ? 0 : metadata.LastModified.Value.ToFileTimeUtc();
        }

        /// <summary>Set the modified time of an existing file to now. </summary>
        public override void TouchFile(string name)
        {
            // I have no idea what the semantics of this should be...
            // we never seem to get called
            _cache.TouchFile(name);
        }

        /// <summary>Removes an existing file in the directory. </summary>
        public override void DeleteFile(string name)
        {
            var location = GetLocation(name);
            _store.Delete(location, _options).WaitAndWrap();
            LeoTrace.WriteLine(String.Format("DELETE {0}", location.BasePath));

            if (_cache.FileExists(name))
            {
                _cache.DeleteFile(name);
            }
        }

        /// <summary>Returns the length of a file in the directory. </summary>
        public override long FileLength(string name)
        {
            var metadata = _store.GetMetadata(GetLocation(name)).ResultAndWrap();
            return metadata == null || !metadata.ContentLength.HasValue ? 0 : metadata.ContentLength.Value;
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput CreateOutput(string name)
        {
            var loc = GetLocation(name);
            return new SecureStoreIndexOutput(_cache, name, async data =>
            {
                // Overwrite metadata for better effiency (size/modified)
                var metadata = new Metadata();
                metadata.ContentLength = data.Metadata.ContentLength;
                metadata.LastModified = data.Metadata.LastModified;

                await _store.SaveData(loc, metadata, data.Stream.CopyToAsync, CancellationToken.None, _encryptor, _options).ConfigureAwait(false);
            });
        }

        /// <summary>Returns a stream reading an existing file. </summary>
        public override IndexInput OpenInput(string name)
        {
            return new SecureStoreIndexInput(this, _cache, _store, _encryptor, GetLocation(name), name);
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
            _cache.ClearLock(name);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                foreach (var l in _locks.Values)
                {
                    l.Dispose();
                }
            }
        }

        public StreamInput OpenCachedInputAsStream(string name)
        {
            return new StreamInput(_cache.OpenInput(name));
        }

        public StreamOutput CreateCachedOutputAsStream(string name)
        {
            return new StreamOutput(_cache.CreateOutput(name));
        }

        private StoreLocation GetLocation(string name)
        {
            return new StoreLocation(_container, Path.Combine(_basePath, name));
        }
    }
}
