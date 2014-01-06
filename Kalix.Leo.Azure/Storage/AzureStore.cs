using Kalix.Leo.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
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
        private readonly bool _enableSnapshots;

        /// <summary>
        /// Constructor for a store backed by Azure
        /// </summary>
        /// <param name="blobStorage">The storage account that backs this store</param>
        /// <param name="enableSnapshots">Whether any save on this store should create snapshots</param>
        /// <param name="deletedKey">The metadata key to check if a store item is soft deleted</param>
        public AzureStore(CloudBlobClient blobStorage, bool enableSnapshots, string deletedKey = null)
        {
            _blobStorage = blobStorage;
            _enableSnapshots = enableSnapshots;
            _deletedKey = deletedKey ?? DefaultDeletedKey;
        }

        public Task SaveData(Stream data, StoreLocation location, IDictionary<string, string> metadata = null)
        {
            return SaveDataInternal(data, location, metadata, false);
        }

        public Task<bool> TryOptimisticWrite(Stream data, StoreLocation location, IDictionary<string, string> metadata = null)
        {
            return SaveDataInternal(data, location, metadata, true);
        }

        private async Task<bool> SaveDataInternal(Stream data, StoreLocation location, IDictionary<string, string> metadata, bool isOptimistic)
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

            try
            {
                var condition = isOptimistic ? AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag) : null;
                using (var writeStream = new BlobBlockStream(blob, null, condition))
                {
                    await data.CopyToAsync(writeStream);
                    await Task.Run(() => writeStream.Close());
                }

                // Create a snapshot straight away on azure
                // Note: this shouldnt matter for cost as any blocks that are the same do not cost extra
                if (_enableSnapshots)
                {
                    await blob.CreateSnapshotAsync();
                }
            }
            catch (StorageException exc)
            {
                if (exc.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                {
                    return false;
                }
                throw;
            }

            return true;
        }

        public async Task<bool> LoadData(StoreLocation location, Func<IDictionary<string, string>, Stream> streamPicker, string snapshot = null)
        {
            var blob = GetBlockBlob(location, snapshot);
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

        public async Task<IEnumerable<Snapshot>> FindSnapshots(StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            var results = await Task.Run(() => blob.Container.ListBlobs(blob.Name, true, BlobListingDetails.Snapshots | BlobListingDetails.Metadata).ToList());

            return results
                .OfType<ICloudBlob>()
                .Where(b => b.Uri == blob.Uri && b.SnapshotTime.HasValue)
                .Select(b => new Snapshot
                {
                    Id = b.SnapshotTime.Value.UtcDateTime.Ticks.ToString(),
                    Modified = b.SnapshotTime.Value.UtcDateTime,
                    Metadata = b.Metadata
                })
                .Reverse() // We know we have to reverse to get right ordering...
                .ToList();
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

        private CloudBlockBlob GetBlockBlob(StoreLocation location, string snapshot = null)
        {
            DateTime? snapshotDate = null;
            if(snapshot != null)
            {
                snapshotDate = new DateTime(long.Parse(snapshot));
            }

            var container = _blobStorage.GetContainerReference(location.Container);
            var offset = snapshotDate.HasValue ? new DateTimeOffset(snapshotDate.Value, new TimeSpan(0)) : (DateTimeOffset?)null;

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

            public override Task FlushAsync(CancellationToken cancellationToken)
            {
                return _writeStream.Value.FlushAsync(cancellationToken);
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

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                var stream = _writeStream.Value;
                if (stream != null)
                {
                    return stream.WriteAsync(buffer, offset, count, cancellationToken);
                }

                return Task.FromResult(0);
            }
        }
    }
}
