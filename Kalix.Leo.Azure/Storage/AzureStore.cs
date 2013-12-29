using Kalix.Leo.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage
{
    public class AzureStore : IStore
    {
        private const string IdExtension = ".dat";
        private const string DefaultDeletedKey = "azurestorage_deleted";

        private readonly CloudBlobClient _blobStorage;
        private readonly string _deletedKey;

        /// <summary>
        /// Constructor for a store backed by Azure
        /// </summary>
        /// <param name="blobStorage">The storage account that backs this store</param>
        /// <param name="deletedKey">The metadata key to check if a store item is soft deleted</param>
        public AzureStore(CloudBlobClient blobStorage, string deletedKey = null)
        {
            _blobStorage = blobStorage;
            _deletedKey = deletedKey ?? DefaultDeletedKey;
        }

        public async Task SaveData(Stream data, StoreLocation location, IDictionary<string, string> metadata = null)
        {
            var blob = GetBlockBlob(location);
            
            // Copy the metadata across
            if (metadata != null)
            {
                foreach (var m in metadata)
                {
                    blob.Metadata[m.Key] = m.Value;
                }
            }

            using (var writeStream = new BlobBlockStream(blob, null))
            {
                await data.CopyToAsync(writeStream);
                await Task.Run(() => writeStream.Close());
            }
        }

        public async Task<bool> LoadData(StoreLocation location, Func<IDictionary<string, string>, Stream> streamPicker)
        {
            var blob = GetBlockBlob(location);
            bool hasDeleted = false;

            var cancellation = new CancellationTokenSource();
            var writeWrapper = new WriteWrapperStreamClass(() =>
            {
                if(blob.Metadata.ContainsKey(_deletedKey))
                {
                    hasDeleted = true;
                    cancellation.Cancel();
                    return null;
                }

                // Should have blob metadata by this point?
                var stream = streamPicker(blob.Metadata);
                if(stream == null)
                {
                    cancellation.Cancel();
                }
                return stream;
            });

            bool hasFile;
            try
            {
                await blob.DownloadToStreamAsync(writeWrapper, cancellation.Token);
                hasFile = true;
            }
            catch(StorageException e)
            {
                if(e.RequestInformation.HttpStatusCode == 404)
                {
                    hasFile = false;
                }
                else
                {
                    throw;
                }
            }
            catch(TaskCanceledException)
            {
                if(hasDeleted)
                {
                    hasFile = false;
                }
                else
                {
                    throw;
                }
            }

            return hasFile;
        }

        public async Task<DateTime?> TakeSnapshot(StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            try
            {
                var snapshot = await blob.CreateSnapshotAsync();
                return snapshot.SnapshotTime.Value.UtcDateTime;
            }
            catch(StorageException e)
            {
                if(e.RequestInformation.HttpStatusCode == 404)
                {
                    return null;
                }

                throw;
            }
        }

        public async Task<IEnumerable<DateTime>> FindSnapshots(StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            var results = await Task.Run(() => blob.Container.ListBlobs(blob.Name, true, BlobListingDetails.Snapshots).ToList());

            return results
                .OfType<ICloudBlob>()
                .Where(b => b.Uri == blob.Uri && b.SnapshotTime.HasValue)
                .Select(b => b.SnapshotTime.Value.UtcDateTime)
                .Reverse()
                .ToList();
        }

        public async Task<bool> LoadSnapshotData(StoreLocation location, DateTime snapshot, Func<IDictionary<string, string>, Stream> streamPicker)
        {
            var blob = GetBlockBlob(location, snapshot);

            var cancellation = new CancellationTokenSource();
            var writeWrapper = new WriteWrapperStreamClass(() =>
            {
                // Should have blob metadata by this point?
                var stream = streamPicker(blob.Metadata);
                if (stream == null)
                {
                    cancellation.Cancel();
                }
                return stream;
            });

            bool hasFile;
            try
            {
                await blob.DownloadToStreamAsync(writeWrapper, cancellation.Token);
                hasFile = true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    hasFile = false;
                }
                else
                {
                    throw;
                }
            }
            return hasFile;
        }

        public async Task SoftDelete(StoreLocation location)
        {
            // In Azure we cannot delete the blob as this will loose the snapshots
            // Instead we will just add some metadata
            var blob = GetBlockBlob(location);
            try
            {
                await blob.FetchAttributesAsync();
            }
            catch (StorageException e)
            {
                if(e.RequestInformation.HttpStatusCode == 404)
                {
                    return;
                }
                throw;
            }

            blob.Metadata.Add(_deletedKey, DateTime.UtcNow.Ticks.ToString());
            await blob.SetMetadataAsync();
        }

        public Task PermanentDelete(StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            return blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, null, null, null);
        }

        public Task CreateContainerIfNotExists(string container)
        {
            var c = _blobStorage.GetContainerReference(container);
            return c.CreateIfNotExistsAsync();
        }

        public Task PermanentDeleteContainer(string container)
        {
            var c = _blobStorage.GetContainerReference(container);
            return c.DeleteIfExistsAsync();
        }

        private CloudBlockBlob GetBlockBlob(StoreLocation location, DateTime? snapshot = null)
        {
            if(snapshot.HasValue && snapshot.Value.Kind == DateTimeKind.Local)
            {
                throw new ArgumentException("Snapshot must be a Utc or Unspecified time", "snapshot");
            }

            var container = _blobStorage.GetContainerReference(location.Container);
            var offset = snapshot.HasValue ? new DateTimeOffset(snapshot.Value, new TimeSpan(0)) : (DateTimeOffset?)null;

            CloudBlockBlob blob;
            if (location.Id.HasValue)
            {
                if (!string.IsNullOrEmpty(location.BasePath))
                {
                    var dir = container.GetDirectoryReference(location.BasePath);
                    blob = dir.GetBlockBlobReference(location.Id.ToString() + IdExtension, offset);
                }
                else
                {
                    blob = container.GetBlockBlobReference(location.Id.ToString() + IdExtension, offset);
                }
            }
            else
            {
                blob = container.GetBlockBlobReference(location.BasePath, offset);
            }

            return blob;
        }

        private class WriteWrapperStreamClass : Stream
        {
            private Lazy<Stream> _writeStream;

            public WriteWrapperStreamClass(Func<Stream> writeStream)
            {
                _writeStream = new Lazy<Stream>(writeStream);
            }

            public override bool CanRead { get { return false; } }
            public override bool CanSeek { get { return false; } }
            public override bool CanWrite { get { return true; } }

            public override void Flush()
            {
                _writeStream.Value.Flush();
            }

            public override long Length
            {
                get { return _writeStream.Value.Length; }
            }

            public override long Position
            {
                get
                {
                    return _writeStream.Value.Position;
                }
                set
                {
                    throw new NotImplementedException();
                }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void SetLength(long value)
            {
                _writeStream.Value.SetLength(value);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                var stream = _writeStream.Value;
                if(stream != null)
                {
                    stream.Write(buffer, offset, count);
                }
            }
        }
    }
}
