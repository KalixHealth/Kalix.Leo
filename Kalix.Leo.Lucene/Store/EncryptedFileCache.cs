using System.IO;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.InteropServices;
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

        public EncryptedFileCache(string directory)
        {
            _directory = directory;

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
                        else
                        {
                            LeoTrace.WriteLine("Skipped blob download");
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

        public Metadata GetMetadata(string key)
        {
            var path = Path.Combine(_directory, key);
            var info = new FileInfo(path);
            if (!info.Exists) { return null; }

            var metadata = new Metadata();
            metadata.LastModified = info.LastWriteTimeUtc;
            metadata.Size = info.Length;

            return metadata;
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
                EncryptFile(directory.FullName);
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
                Directory.Delete(_directory, true); // Delete the cache entirely
                _isDisposed = true;
            }
        }
    }
}
