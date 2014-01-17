using AsyncBridge;
using Lucene.Net.Store;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Store
{
    public class SecureStoreIndexInput : IndexInput
    {
        private readonly IFileCache _cache;
        private readonly string _cachePath;
        private readonly Lazy<Stream> _stream;

        private bool _hasCloned;
        private bool _isDisposed;

        public SecureStoreIndexInput(IFileCache cache, ISecureStore store, StoreLocation location, string cachePath)
        {
            _cache = cache;
            _cachePath = cachePath;

            var hasFile = GetSyncVal(_cache.UpdateIfModified(_cachePath, store.LoadData(location)));
            if(!hasFile)
            {
                throw new FileNotFoundException("Input file does not exist: " + cachePath);
            }

            _stream = new Lazy<Stream>(() => 
                GetSyncVal(_cache
                    .GetReadWriteStream(_cachePath)
                    .ContinueWith(t => new BufferedStream(t.Result)))
            );  
        }

        // Cloning method, makes sure that it is still trying to load from the same data/cache
        // however the pointer can be different!
        protected SecureStoreIndexInput(IFileCache cache, string cachePath, long pointer)
        {
            _cache = cache;
            _cachePath = cachePath;

            _stream = new Lazy<Stream>(() =>
                GetSyncVal(_cache
                    .GetReadWriteStream(_cachePath)
                    .ContinueWith(t => new BufferedStream(t.Result)))
            );  
        }

        public override long FilePointer
        {
            get { return _stream.Value.Position; }
        }

        public override long Length()
        {
            return _stream.Value.Length;
        }

        public override byte ReadByte()
        {
            return (byte)_stream.Value.ReadByte();
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            _stream.Value.Read(b, offset, len);
        }

        public override void Seek(long pos)
        {
            _stream.Value.Seek(pos, SeekOrigin.Begin);
        }

        public override object Clone()
        {
            _hasCloned = true;
            return new SecureStoreIndexInput(_cache, _cachePath, FilePointer);
        }

        protected override void Dispose(bool disposing)
        {
            // Only dispose update task if we havent cloned...
            if(!_isDisposed && !_hasCloned)
            {
                if(_stream.IsValueCreated)
                {
                    _stream.Value.Dispose();
                }

                _isDisposed = true;
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

                try
                {
                    using (var w = AsyncHelper.Wait)
                    {
                        w.Run(task.ContinueWith(t => { val = t.Result; }));
                    }
                }
                catch(AggregateException e)
                {
                    Exception ex = e;
                    // Unwrap aggregate exceptions
                    while(ex is AggregateException)
                    {
                        ex = ex.InnerException;
                    }
                    throw ex;
                }
            }
            return val;
        }
    }
}
