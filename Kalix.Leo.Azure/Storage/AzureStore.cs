using Kalix.Leo.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
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
        private readonly bool _trace;

        /// <summary>
        /// Constructor for a store backed by Azure
        /// </summary>
        /// <param name="blobStorage">The storage account that backs this store</param>
        /// <param name="enableSnapshots">Whether any save on this store should create snapshots</param>
        /// <param name="deletedKey">The metadata key to check if a store item is soft deleted</param>
        public AzureStore(CloudBlobClient blobStorage, bool enableSnapshots, string deletedKey = null, bool trace = false)
        {
            _blobStorage = blobStorage;
            _enableSnapshots = enableSnapshots;
            _deletedKey = deletedKey ?? DefaultDeletedKey;
            _trace = trace;
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
                if (_trace)
                {
                    Trace.WriteLine("Leased Blob: " + blob.Name);
                }
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
                try
                {
                    using (var stream = new MemoryStream(new byte[1]))
                    {
                        await blob.UploadFromStreamAsync(stream).ConfigureAwait(false);
                    }
                    leaseId = await blob.AcquireLeaseAsync(TimeSpan.FromMinutes(1), null).ConfigureAwait(false);
                    if (_trace)
                    {
                        Trace.WriteLine("Created new blob and lease (2 calls): " + blob.Name);
                    }
                }
                catch (StorageException e)
                {
                    // If we have a conflict this blob is already locked...
                    if (e.RequestInformation.HttpStatusCode == 409)
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
            }

            var condition = AccessCondition.GenerateLeaseCondition(leaseId);
            var release = Disposable.Create(() => { blob.ReleaseLease(condition); });

            // Every 40 secs keep the lock renewed
            var keepAlive = Observable
                .Interval(TimeSpan.FromSeconds(40), Scheduler.Default)
                .Subscribe(
                    _ => 
                    { 
                        blob.RenewLease(condition);
                        if (_trace)
                        {
                            Trace.WriteLine("Renewed Lease: " + blob.Name);
                        }
                    }, 
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
                    .BufferBytes(AzureBlockSize, true)
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
                                    if (_trace)
                                    {
                                        Trace.WriteLine("Put Block '" + blockStr + "': " + blob.Name);
                                    }
                                }
                                bag.Add(blockStr);
                            }

                            // We are doing multipart here!
                            blockStr = Convert.ToBase64String(BitConverter.GetBytes(pNum));
                            using (var ms = new MemoryStream(b))
                            {
                                await blob.PutBlockAsync(blockStr, ms, null, condition, null, null).ConfigureAwait(false);
                                if (_trace)
                                {
                                    Trace.WriteLine("Put Block '" + blockStr + "': " + blob.Name);
                                }
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
                    if (_trace)
                    {
                        Trace.WriteLine("Finished Put Blocks: " + blob.Name);
                    }
                }
                else
                {
                    // If we didnt do multimerge then just do single!
                    using (var ms = new MemoryStream(firstHit))
                    {
                        await blob.UploadFromStreamAsync(ms, condition, null, null).ConfigureAwait(false);
                        if (_trace)
                        {
                            Trace.WriteLine("Uploaded Single Block: " + blob.Name);
                        }
                    }
                }

                // Create a snapshot straight away on azure
                // Note: this shouldnt matter for cost as any blocks that are the same do not cost extra
                if (_enableSnapshots)
                {
                    await blob.CreateSnapshotAsync();
                    if (_trace)
                    {
                        Trace.WriteLine("Created Snapshot: " + blob.Name);
                    }
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

        public async Task<Metadata> GetMetadata(StoreLocation location, string snapshot = null)
        {
            var blob = GetBlockBlob(location, snapshot);
            try
            {
                await blob.FetchAttributesAsync();
                if (_trace)
                {
                    Trace.WriteLine("Got Metdata: " + blob.Name);
                }
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    return null;
                }

                throw;
            }

            var metadata = GetActualMetadata(blob);
            if (metadata.ContainsKey(_deletedKey))
            {
                metadata = null;
            }
            return metadata;
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
                            if (_trace)
                            {
                                Trace.WriteLine("Downloading blob: " + blob.Name);
                            }
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

            data
                .Do(_ => { }, (e) => source.TrySetException(e), () => { }) // Catch any loose exceptions
                .Subscribe(cancellation.Token);

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
                    Metadata = GetActualMetadata(b)
                });
        }

        public IObservable<LocationWithMetadata> FindFiles(string container, string prefix = null)
        {
            var results = Observable.Create<ICloudBlob>((observer, ct) =>
            {
                var c = _blobStorage.GetContainerReference(SafeContainerName(container));
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
                    
                    var loc = new StoreLocation(container, path, id);
                    return new LocationWithMetadata(loc, GetActualMetadata(b));
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
                    if (_trace)
                    {
                        Trace.WriteLine("Listed blob segment for prefix: " + prefix);
                    }

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
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    // If we get an error we do not have any blobs!
                    observer.OnCompleted();
                }
                else
                {
                    observer.OnError(e);
                }
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
            if (_trace)
            {
                Trace.WriteLine("Soft deleted (2 calls): " + blob.Name);
            }
        }

        public Task PermanentDelete(StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            if (_trace)
            {
                Trace.WriteLine("Deleted blob: " + blob.Name);
            }
            return blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, null, null, null);
        }

        public Task CreateContainerIfNotExists(string container)
        {
            container = SafeContainerName(container);
            if (_trace)
            {
                Trace.WriteLine("Trying to create container: " + container);
            }

            var c = _blobStorage.GetContainerReference(container);
            return c.CreateIfNotExistsAsync();
        }

        public Task PermanentDeleteContainer(string container)
        {
            container = SafeContainerName(container);
            if (_trace)
            {
                Trace.WriteLine("Trying to delete container: " + container);
            }

            var c = _blobStorage.GetContainerReference(container);
            return c.DeleteIfExistsAsync();
        }

        private Metadata GetActualMetadata(ICloudBlob blob)
        {
            var metadata = new Metadata(blob.Metadata);

            if (!metadata.ContainsKey(MetadataConstants.ModifiedMetadataKey) && (blob.Properties.LastModified.HasValue || blob.SnapshotTime.HasValue))
            {
                metadata[MetadataConstants.ModifiedMetadataKey] = blob.SnapshotTime.HasValue ? blob.SnapshotTime.Value.UtcTicks.ToString() : blob.Properties.LastModified.Value.UtcTicks.ToString();
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

            var container = _blobStorage.GetContainerReference(SafeContainerName(location.Container));
            var offset = snapshotDate.HasValue ? new DateTimeOffset(snapshotDate.Value, new TimeSpan(0)) : (DateTimeOffset?)null;

            CloudBlockBlob blob;
            if (location.Id.HasValue)
            {
                if (!string.IsNullOrEmpty(location.BasePath))
                {
                    var dir = container.GetDirectoryReference(SafePath.MakeSafeFilePath(location.BasePath));
                    blob = dir.GetBlockBlobReference(location.Id.ToString() + IdExtension, offset);
                }
                else
                {
                    blob = container.GetBlockBlobReference(location.Id.ToString() + IdExtension, offset);
                }
            }
            else
            {
                blob = container.GetBlockBlobReference(SafePath.MakeSafeFilePath(location.BasePath), offset);
            }

            return blob;
        }

        private string SafeContainerName(string containerName)
        {
            if(containerName.Length > 63)
            {
                throw new ArgumentException("Container name cannot be longer than 63 chars", "containerName");
            }

            if(containerName.Length < 3)
            {
                containerName = containerName.PadLeft(3, '0');
            }

            return containerName;
        }
    }
}
