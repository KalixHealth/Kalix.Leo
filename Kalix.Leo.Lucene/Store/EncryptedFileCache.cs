using Kalix.Leo.Storage;
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

        public EncryptedFileCache()
        {
            _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

            Directory.CreateDirectory(_directory);
            if (!EncryptFile(_directory))
            {
                throw new FileLoadException("Could not set encryption on directory");
            }
        }

        public async Task UpdateIfModified(string key, Task<DataWithMetadata> data = null)
        {
            var path = Path.Combine(_directory, key);
            if(data == null)
            {
                // Create an empty file
                File.WriteAllBytes(path, new byte[0]);
            }
            else
            {
                var info = new FileInfo(path);
                using (var actualData = await data.ConfigureAwait(false))
                {
                    if (!actualData.Metadata.LastModified.HasValue || !info.Exists || info.LastWriteTimeUtc < actualData.Metadata.LastModified.Value)
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
                        }
                }
            }
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

            var fs = info.OpenRead();
            if (initialPosition > 0) { fs.Seek(initialPosition, SeekOrigin.Begin); }

            return Task.FromResult<Stream>(fs);
        }

        public Task Delete(string key)
        {
            var path = Path.Combine(_directory, key);
            File.Delete(path);
            return Task.FromResult(0);
        }

        public void Dispose()
        {
            if (_isDisposed)
            {
                Directory.Delete(_directory);
                _isDisposed = true;
            }
        }
    }
}
