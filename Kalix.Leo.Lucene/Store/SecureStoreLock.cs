using AsyncBridge;
using Kalix.Leo.Storage;
using Lucene.Net.Store;
using System;

namespace Kalix.Leo.Lucene.Store
{
    public class SecureStoreLock : Lock, IDisposable
    {
        private readonly ISecureStore _store;
        private readonly StoreLocation _location;

        private IDisposable _lock;

        public SecureStoreLock(ISecureStore store, StoreLocation location)
        {
            _store = store;
            _location = location;
        }

        public override bool IsLocked()
        {
            // Make this method fast... if there actually is a lock we will find out on 'obtain'
            return _lock != null;
        }

        public override bool Obtain()
        {
            if (_lock == null)
            {
                using (var w = AsyncHelper.Wait)
                {
                    w.Run(_store.Lock(_location).ContinueWith(t => { _lock = t.Result; }));
                }
            }

            return _lock != null;
        }

        public override void Release()
        {
            if(_lock != null)
            {
                _lock.Dispose();
                _lock = null;
            }
        }

        public void Dispose()
        {
            Release();
        }
    }
}
