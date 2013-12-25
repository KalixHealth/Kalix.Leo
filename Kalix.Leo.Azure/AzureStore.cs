using Kalix.Leo.Azure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure
{
    public class AzureStore : IStore
    {
        private const string IdExtension = ".dat";

        private readonly CloudBlobClient _blobStorage;

        public AzureStore(CloudBlobClient blobStorage)
        {
            _blobStorage = blobStorage;
        }

        public async Task SaveData(Stream data, StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            using (var writeStream = new BlobBlockStream(blob))
            {
                await data.CopyToAsync(writeStream);
                await Task.Run(() => writeStream.Close());
            }
        }

        public Task<Stream> LoadData(StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            blob.DownloadToStreamAsync()
        }

        public Task<DateTime> TakeSnapshot(StoreLocation location)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<DateTime>> FindAllSnapshots(DateTime? earilest = null, DateTime? latest = null, int? take = 10)
        {
            throw new NotImplementedException();
        }

        public Task<System.IO.Stream> LoadSnapshotData(StoreLocation location, DateTime snapshot)
        {
            throw new NotImplementedException();
        }

        public Task SoftDelete(StoreLocation location)
        {
            throw new NotImplementedException();
        }

        public Task PermanentDelete(StoreLocation location)
        {
            throw new NotImplementedException();
        }

        public Task CreateContainerIfNotExists(string container)
        {
            throw new NotImplementedException();
        }

        public Task PermanentDeleteContainer(string container)
        {
            throw new NotImplementedException();
        }

        private CloudBlockBlob GetBlockBlob(StoreLocation location)
        {
            var container = _blobStorage.GetContainerReference(location.Container);

            CloudBlockBlob blob;
            if (location.Id.HasValue)
            {
                if (!string.IsNullOrEmpty(location.BasePath))
                {
                    var dir = container.GetDirectoryReference(location.BasePath);
                    blob = dir.GetBlockBlobReference(location.Id.ToString() + IdExtension);
                }
                else
                {
                    blob = container.GetBlockBlobReference(location.Id.ToString() + IdExtension);
                }
            }
            else
            {
                blob = container.GetBlockBlobReference(location.BasePath);
            }

            return blob;
        }
    }
}
