using AsyncBridge;
using Lucene.Net.Store;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Store
{
    /// <summary>
    /// Implements IndexOutput semantics for a write/append only file
    /// </summary>
    public class SecureStoreIndexOutput : IndexOutput
    {
        private static long ticks1970 = new DateTime(1970, 1, 1, 0, 0, 0).Ticks / TimeSpan.TicksPerMillisecond;

        private Directory _cache;
        private string _name;
        private IndexOutput _indexOutput;
        private Mutex _fileMutex;

        private Func<DataWithMetadata, Task> _saveTask;

        public SecureStoreIndexOutput(Directory cache, string cachePath, Func<DataWithMetadata, Task> saveTask)
        {
            _cache = cache;
            _name = cachePath;
            _saveTask = saveTask;

            _fileMutex = BlobMutexManager.GrabMutex(_name);
            _fileMutex.WaitOne();
            try
            {
                // create the local cache one we will operate against...
                _indexOutput = _cache.CreateOutput(_name);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override void Flush()
        {
            _indexOutput.Flush();
        }

        protected override void Dispose(bool disposing)
        {
            _fileMutex.WaitOne();
            try
            {
                // make sure it's all written out
                _indexOutput.Flush();

                long originalLength = _indexOutput.Length;
                _indexOutput.Dispose();

                var blobStream = new StreamInput(_cache.OpenInput(_name));
                
                try
                {
                    var elapsed = _cache.FileModified(_name);

                    // normalize RAMDirectory and FSDirectory times
                    if (elapsed > ticks1970)
                    {
                        elapsed -= ticks1970;
                    }

                    var cachedLastModifiedUTC = new DateTime(elapsed, DateTimeKind.Local).ToUniversalTime();

                    var data = new DataWithMetadata(blobStream, new Metadata
                    {
                        Size = originalLength,
                        LastModified = cachedLastModifiedUTC
                    });

                    using (var w = AsyncHelper.Wait)
                    {
                        w.Run(_saveTask(data));
                    }

                    LeoTrace.WriteLine(string.Format("PUT {1} bytes to {0} in cloud", _name, blobStream.Length));
                }
                finally
                {
                    blobStream.Dispose();
                }

                // clean up
                _indexOutput = null;
                _cache = null;
                GC.SuppressFinalize(this);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override long Length
        {
            get
            {
                return _indexOutput.Length;
            }
        }

        public override void WriteByte(byte b)
        {
            _indexOutput.WriteByte(b);
        }

        public override void WriteBytes(byte[] b, int length)
        {
            _indexOutput.WriteBytes(b, length);
        }

        public override void WriteBytes(byte[] b, int offset, int length)
        {
            _indexOutput.WriteBytes(b, offset, length);
        }

        public override long FilePointer
        {
            get
            {
                return _indexOutput.FilePointer;
            }
        }

        public override void Seek(long pos)
        {
            _indexOutput.Seek(pos);
        }
    }
}
