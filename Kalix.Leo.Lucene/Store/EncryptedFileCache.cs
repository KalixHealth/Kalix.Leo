using Kalix.Leo.Storage;
using System.IO;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Store
{
    public class EncryptedFileCache : IFileCache
    {
        // Need to use the windows api to create an encrypted directory
        [DllImport("advapi32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern bool EncryptFile(string filename);

        private readonly string _directory;

        private bool _isDisposed;

        public EncryptedFileCache()
        {
            _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

            CheckDirectoryExists(new DirectoryInfo(_directory));
        }

        public async Task<bool> UpdateIfModified(string key, Task<DataWithMetadata> data = null)
        {
            var result = false;
            var path = Path.Combine(_directory, key);
            if(data == null)
            {
                // Create an empty file
                File.WriteAllBytes(path, new byte[0]);
            }
            else
            {
                var info = new FileInfo(path);
                CheckDirectoryExists(info.Directory);

                var actualData = await data;
                if(actualData == null)
                {
                    // Create an empty file
                    File.WriteAllBytes(path, new byte[0]);
                }
                else
                {
                    using (actualData)
                    {
                        if (!actualData.Metadata.LastModified.HasValue || !info.Exists || info.LastWriteTimeUtc < actualData.Metadata.LastModified.Value)
                        {
                            using (var fs = info.OpenWrite())
                            {
                                await actualData
                                    .Stream
                                    .Select(async b =>
                                    {
                                        await fs.WriteAsync(b, 0, b.Length).ConfigureAwait(false);
                                        return Unit.Default;
                                    })
                                    .Merge();

                                result = true;
                            }
                        }
                    }
                }
            }
            return result;
        }

        public Task<DataWithMetadata> LoadAllData(string key)
        {
            var path = Path.Combine(_directory, key);
            var info = new FileInfo(path);

            var metadata = new Metadata();
            metadata.LastModified = info.LastWriteTimeUtc;
            metadata.Size = info.Length;

            var stream = Observable.Create<byte[]>(obs =>
            {
                var fs = info.OpenRead();

                // Read file in 4mb blocks
                var sub = fs.ToObservable(4194304).Subscribe(obs);

                return new CompositeDisposable(fs, sub);
            });

            return Task.FromResult(new DataWithMetadata(stream, metadata));
        }

        public Task<Stream> GetReadWriteStream(string key, long initialPosition = 0)
        {
            var path = Path.Combine(_directory, key);
            var info = new FileInfo(path);
            CheckDirectoryExists(info.Directory);

            var fs = info.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
            if (initialPosition > 0) { fs.Seek(initialPosition, SeekOrigin.Begin); }

            return Task.FromResult<Stream>(fs);
        }

        public Task<Stream> GetReadonlyStream(string key, long initialPosition = 0)
        {
            var path = Path.Combine(_directory, key);
            var info = new FileInfo(path);
            CheckDirectoryExists(info.Directory);

            var fs = info.Open(FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            if (initialPosition > 0) { fs.Seek(initialPosition, SeekOrigin.Begin); }

            return Task.FromResult<Stream>(fs);
        }

        private static void CheckDirectoryExists(DirectoryInfo directory)
        {
            if (!directory.Exists)
            {
                directory.Create();
                if (!EncryptFile(directory.FullName))
                {
                    throw new FileLoadException("Could not set encryption on directory: " + directory.FullName);
                }
            }
        }

        public Task Delete(string key)
        {
            var path = Path.Combine(_directory, key);
            var info = new FileInfo(path);
            if (info.Exists)
            {
                info.Delete();
            }
            return Task.FromResult(0);
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                // This is ugly... i know :(
                // Only way to retry... this should only happen when things are shutting down though...
                int retries = 3;
                while (true)
                {
                    try
                    {
                        Directory.Delete(_directory, true); // Delete the cache entirely
                        break; // success!
                    }
                    catch
                    {
                        if (--retries == 0) throw;
                        else Thread.Sleep(1000);
                    }
                }
                
                _isDisposed = true;
            }
        }
    }
}
