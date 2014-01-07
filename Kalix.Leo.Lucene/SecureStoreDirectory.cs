using Lucene.Net.Store;
using System;

namespace Kalix.Leo.Lucene
{
    public class SecureStoreDirectory : Directory
    {
        private readonly ISecureStore _store;
        private readonly string _container;
        private readonly Directory _cachingDirectory;
        private readonly SecureStoreOptions _options;

        public SecureStoreDirectory(ISecureStore store, string container, Directory cachingDirectory)
        {
            _container = container;
            _cachingDirectory = cachingDirectory;
            _store = store;
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
            _store.Delete(GetLocation(name), SecureStoreOptions.None);
            _cachingDirectory.DeleteFile(name);
        }

        protected override void Dispose(bool disposing)
        {
            throw new NotImplementedException();
        }

        public override bool FileExists(string name)
        {
            throw new NotImplementedException();
        }

        public override long FileLength(string name)
        {
            throw new NotImplementedException();
        }

        public override long FileModified(string name)
        {
            throw new NotImplementedException();
        }

        public override string[] ListAll()
        {
            throw new NotImplementedException();
        }

        public override void TouchFile(string name)
        {
            throw new NotImplementedException();
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
    }
}
