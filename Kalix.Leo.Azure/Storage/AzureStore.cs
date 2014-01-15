using Kalix.Leo.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage
{
    public class AzureStore : IOptimisticStore
    {
        private const string IdExtension = ".id";
        private const string DefaultDeletedKey = "leodeleted";
        private const int AzureBlockSize = 4194304;

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

        public Task SaveData(StoreLocation location, DataWithMetadata data)
        {
            return SaveDataInternal(location, data, false);
        }

        public Task<bool> TryOptimisticWrite(StoreLocation location, DataWithMetadata data)
        {
            return SaveDataInternal(location, data, true);
        }

        public async Task<IDisposable> Lock(StoreLocation location)
        {
            var blob = GetBlockBlob(location);

            string leaseId;

            try
            {
                leaseId = await blob.AcquireLeaseAsync(TimeSpan.FromMinutes(1), null).ConfigureAwait(false);
            }
            catch (StorageException e)
            {
                // If we have a conflict this blob is already locked...
                if(e.RequestInformation.HttpStatusCode == 409)
                {
                    return null;
                }

                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    leaseId = null;
                }
                else
                {
                    throw;
                }
            }

            // May not have had a blob pushed...
            if (leaseId == null)
            {
                using (var stream = new MemoryStream(new byte[1]))
                {
                    await blob.UploadFromStreamAsync(stream).ConfigureAwait(false);
                }
                leaseId = await blob.AcquireLeaseAsync(TimeSpan.FromMinutes(1), null).ConfigureAwait(false);
            }

            var condition = AccessCondition.GenerateLeaseCondition(leaseId);
            var release = Disposable.Create(() => { blob.ReleaseLease(condition); });

            // Every 40 secs keep the lock renewed
            var keepAlive = Observable
                .Interval(TimeSpan.FromSeconds(40), Scheduler.Default)
                .Subscribe(
                    _ => { blob.RenewLease(condition); }, 
                    () => { release.Dispose(); }
                );


            return new CompositeDisposable(keepAlive, release);
        }

        private async Task<bool> SaveDataInternal(StoreLocation location, DataWithMetadata data, bool isOptimistic)
        {
            try
            {
                var blob = GetBlockBlob(location);

                // Copy the metadata across
                foreach (var m in data.Metadata)
                {
                    blob.Metadata[m.Key] = m.Value;
                }

                var condition = isOptimistic ? AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag) : null;

                byte[] firstHit = null;
                var bag = new ConcurrentBag<string>();
                int partNumber = 1;

                await data.Stream
                    .BufferBytes(AzureBlockSize)
                    .Select(async b =>
                    {
                        if (firstHit == null)
                        {
                            firstHit = b;
                        }
                        else
                        {
                            var pNum = Interlocked.Increment(ref partNumber);

                            string blockStr;
                            if (pNum == 2)
                            {
                                blockStr = Convert.ToBase64String(BitConverter.GetBytes(1));
                                using (var ms = new MemoryStream(firstHit))
                                {
                                    await blob.PutBlockAsync(blockStr, ms, null, condition, null, null).ConfigureAwait(false);
                                }
                                bag.Add(blockStr);
                            }

                            // We are doing multipart here!
                            blockStr = Convert.ToBase64String(BitConverter.GetBytes(pNum));
                            using (var ms = new MemoryStream(b))
                            {
                                await blob.PutBlockAsync(blockStr, ms, null, condition, null, null).ConfigureAwait(false);
                            }
                            bag.Add(blockStr);
                        }

                        return Unit.Default;
                    })
                    .Merge();

                // Did we do multimerge or not?
                if (bag.Any())
                {
                    await blob.PutBlockListAsync(bag).ConfigureAwait(false);
                }
                else
                {
                    // If we didnt do multimerge then just do single!
                    using (var ms = new MemoryStream(firstHit))
                    {
                        await blob.UploadFromStreamAsync(ms, condition, null, null).ConfigureAwait(false);
                    }
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

        public async Task<IMetadata> GetMetadata(StoreLocation location, string snapshot = null)
        {
            var blob = GetBlockBlob(location, snapshot);
            try
            {
                await blob.FetchAttributesAsync();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    return null;
                }

                throw;
            }

            return GetActualMetadata(blob);
        }

        public Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null)
        {
            var blob = GetBlockBlob(location, snapshot);

            var cancellation = new CancellationTokenSource();
            var source = new TaskCompletionSource<DataWithMetadata>();

            IObservable<byte[]> data = null;
            data = Observable.Create<byte[]>((obs, ct) =>
            {
                return obs.UseWriteStream(
                    async s =>
                    {
                        try
                        {
                            await blob.DownloadToStreamAsync(s, ct);
                        }
                        catch (StorageException e)
                        {
                            if (e.RequestInformation.HttpStatusCode == 404)
                            {
                                source.TrySetResult(null);
                            }
                            else
                            {
                                throw;
                            }
                        }
                    },
                    () =>
                    {
                        var metadata = GetActualMetadata(blob);
                        if (metadata.ContainsKey(_deletedKey))
                        {
                            source.TrySetResult(null);
                            cancellation.Cancel();
                        }
                        else
                        {
                            source.TrySetResult(new DataWithMetadata(data, metadata));
                        }
                    }
                );
            });

            data.Subscribe(cancellation.Token);
            return source.Task;
        }

        public IObservable<Snapshot> FindSnapshots(StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            var results = Observable.Create<ICloudBlob>((observer, ct) =>
            {
                return ListBlobs(observer, blob.Container, blob.Name, BlobListingDetails.Snapshots | BlobListingDetails.Metadata, ct);
            });

            return results
                .Where(b => b.Uri == blob.Uri && b.SnapshotTime.HasValue)
                .Select(b => new Snapshot
                {
                    Id = b.SnapshotTime.Value.UtcDateTime.Ticks.ToString(),
                    Modified = b.SnapshotTime.Value.UtcDateTime,
                    Metadata = GetActualMetadata(b)
                });
        }

        public IObservable<StoreLocation> FindFiles(string container, string prefix = null)
        {
            var results = Observable.Create<ICloudBlob>((observer, ct) =>
            {
                var c = _blobStorage.GetContainerReference(container);
                return ListBlobs(observer, c, prefix, BlobListingDetails.Metadata, ct);
            });

            return results
                .Where(b => !b.Metadata.ContainsKey(_deletedKey)) // Do not include blobs which are soft deleted
                .Select(b =>
                {
                    long? id = null;
                    string path = b.Name;
                    if (path.EndsWith(IdExtension))
                    {
                        long tempId;
                        if (long.TryParse(Path.GetFileNameWithoutExtension(path), out tempId))
                        {
                            id = tempId;
                            path = Path.GetDirectoryName(path);
                        }
                    }
                    return new StoreLocation(container, path, id);
                });
        }

        private async Task ListBlobs(IObserver<ICloudBlob> observer, CloudBlobContainer container, string prefix, BlobListingDetails options, CancellationToken ct)
        {
            BlobContinuationToken token = new BlobContinuationToken();

            try
            {
                do
                {
                    var segment = await container.ListBlobsSegmentedAsync(prefix, true, options, null, token, null, null, ct);

                    foreach (var blob in segment.Results.OfType<ICloudBlob>())
                    {
                        observer.OnNext(blob);
                    }

                    token = segment.ContinuationToken;
                }
                while (token != null && !ct.IsCancellationRequested);

                ct.ThrowIfCancellationRequested(); // Just in case...
                observer.OnCompleted();
            }
            catch (Exception e)
            {
                observer.OnError(e);
            }
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
                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    return;
                }
                throw;
            }

            blob.Metadata[_deletedKey] = DateTime.UtcNow.Ticks.ToString();
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

        private IMetadata GetActualMetadata(ICloudBlob blob)
        {
            var metadata = new Metadata(blob.Metadata);

            if (!metadata.ContainsKey(MetadataConstants.ModifiedMetadataKey) && blob.Properties.LastModified.HasValue)
            {
                metadata[MetadataConstants.ModifiedMetadataKey] = blob.Properties.LastModified.Value.UtcDateTime.Ticks.ToString();
            }

            if (!metadata.ContainsKey(MetadataConstants.SizeMetadataKey))
            {
                metadata[MetadataConstants.SizeMetadataKey] = blob.Properties.Length.ToString();
            }

            return metadata;
        }

        private CloudBlockBlob GetBlockBlob(StoreLocation location, string snapshot = null)
        {
            DateTime? snapshotDate = null;
            if (snapshot != null)
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
    }
}
