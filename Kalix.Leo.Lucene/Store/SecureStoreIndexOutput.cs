using AsyncBridge;
using Kalix.Leo.Storage;
using Lucene.Net.Store;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Store
{
    public class SecureStoreIndexOutput : IndexOutput
    {
        private readonly IFileCache _cache;
        private readonly Func<DataWithMetadata, Task> _saveTask;
        private readonly string _cachePath;
        private readonly Lazy<Stream> _stream;

        private bool _isDisposed;

        public SecureStoreIndexOutput(IFileCache cache, string cachePath, Func<DataWithMetadata, Task> saveTask)
        {
            _cache = cache;
            _cachePath = cachePath;
            _saveTask = saveTask;
            _stream = new Lazy<Stream>(() => GetSyncVal(_cache.GetReadWriteStream(_cachePath)));
        }

        public override long FilePointer
        {
            get { return _stream.Value.Position; }
        }

        public override void Flush()
        {
            _stream.Value.Flush();
        }

        public override long Length
        {
            get 
            {
                return _stream.Value.Length;
            }
        }

        public override void Seek(long pos)
        {
            _stream.Value.Seek(pos, SeekOrigin.Begin);
        }

        public override void WriteByte(byte b)
        {
            _stream.Value.WriteByte(b);
        }

        public override void WriteBytes(byte[] b, int offset, int length)
        {
            _stream.Value.Write(b, offset, length);
        }

        protected override void Dispose(bool disposing)
        {
            if(!_isDisposed)
            {
                // Close the stream off...
                if(_stream.IsValueCreated)
                {
                    _stream.Value.Flush();
                    _stream.Value.Dispose();
                }

                // Make sure to save it all back down
                using (var w = AsyncHelper.Wait)
                {
                    w.Run(CopyDataFromCache());
                }

                _isDisposed = true;
            }
        }

        private async Task CopyDataFromCache()
        {
            using(var data = await _cache.LoadAllData(_cachePath))
            {
                await _saveTask(data);
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
