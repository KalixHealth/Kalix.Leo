using Kalix.Leo.Encryption;
using Kalix.Leo.Storage;
using Lucene.Net.Store;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Store
{
    public class SecureStoreIndexInput : IndexInput
    {
        private SecureStoreDirectory _directory;
        private Directory _cache;
        private string _name;

        private IndexInput _indexInput;
        private Mutex _fileMutex;

        private static long ticks1970 = new DateTime(1970, 1, 1, 0, 0, 0).Ticks / TimeSpan.TicksPerMillisecond;

        public SecureStoreIndexInput(SecureStoreDirectory directory, Directory cache, ISecureStore store, IEncryptor encryptor, StoreLocation location, string cachePath)
        {
            _directory = directory;
            _cache = cache;
            _name = cachePath;

            _fileMutex = BlobMutexManager.GrabMutex(_name);
            _fileMutex.WaitOne();
            try
            {
                InitialiseFile(store, encryptor, location).WaitAndWrap();
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        private async Task InitialiseFile(ISecureStore store, IEncryptor encryptor, StoreLocation location)
        {
            bool fFileNeeded = false;
            if (!_cache.FileExists(_name))
            {
                fFileNeeded = true;
            }
            else
            {
                long cachedLength = _cache.FileLength(_name);

                var metadata = await store.GetMetadata(location).ConfigureAwait(false);
                if (metadata == null)
                {
                    throw new System.IO.FileNotFoundException(_name);
                }

                var blobLength = metadata.ContentLength ?? 0;
                var blobLastModifiedUTC = metadata.LastModified ?? DateTime.UtcNow;

                if (cachedLength != blobLength)
                {
                    fFileNeeded = true;
                }
                else
                {
                    // there seems to be an error of 1 tick which happens every once in a while 
                    // for now we will say that if they are within 1 tick of each other and same length 
                    var elapsed = _cache.FileModified(_name);

                    // normalize RAMDirectory and FSDirectory times
                    if (elapsed > ticks1970)
                    {
                        elapsed -= ticks1970;
                    }

                    var cachedLastModifiedUTC = new DateTime(elapsed, DateTimeKind.Local).ToUniversalTime();
                    if (cachedLastModifiedUTC != blobLastModifiedUTC)
                    {
                        var timeSpan = blobLastModifiedUTC.Subtract(cachedLastModifiedUTC);
                        if (timeSpan.TotalSeconds > 1)
                        {
                            fFileNeeded = true;
                        }
                    }
                }
            }

            // if the file does not exist
            // or if it exists and it is older then the lastmodified time in the blobproperties (which always comes from the blob storage)
            if (fFileNeeded)
            {
                using (StreamOutput fileStream = _directory.CreateCachedOutputAsStream(_name))
                {
                    var data = await store.LoadData(location, null, encryptor).ConfigureAwait(false);
                    if (data == null)
                    {
                        throw new System.IO.FileNotFoundException(_name);
                    }
                    await data.Stream.CopyToStream(fileStream, CancellationToken.None).ConfigureAwait(false);
                }

                // and open it as an input 
                _indexInput = _cache.OpenInput(_name);
            }
            else
            {
                // open the file in read only mode
                _indexInput = _cache.OpenInput(_name);
            }
        }

        private SecureStoreIndexInput(SecureStoreIndexInput cloneInput)
        {
            _fileMutex = BlobMutexManager.GrabMutex(cloneInput._name);
            _fileMutex.WaitOne();

            try
            {
                _directory = cloneInput._directory;
                _cache = cloneInput._cache;
                _name = cloneInput._name;
                _indexInput = cloneInput._indexInput.Clone() as IndexInput;
            }
            catch (Exception)
            {
                // sometimes we get access denied on the 2nd stream...but not always. I haven't tracked it down yet
                // but this covers our tail until I do
                LeoTrace.WriteLine(String.Format("Falling back to memory clone for {0}", cloneInput._name));
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override byte ReadByte()
        {
            return _indexInput.ReadByte();
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            _indexInput.ReadBytes(b, offset, len);
        }

        public override long FilePointer
        {
            get
            {
                return _indexInput.FilePointer;
            }
        }

        public override void Seek(long pos)
        {
            _indexInput.Seek(pos);
        }

        protected override void Dispose(bool disposing)
        {
            _fileMutex.WaitOne();
            try
            {
                _indexInput.Dispose();
                _indexInput = null;
                _directory = null;
                _cache = null;
                GC.SuppressFinalize(this);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override long Length()
        {
            return _indexInput.Length();
        }

        public override object Clone()
        {
            IndexInput clone = null;
            try
            {
                _fileMutex.WaitOne();
                clone = new SecureStoreIndexInput(this);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
            return clone;
        }
    }
}
