using Kalix.Leo.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
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

        public Task SaveData(StoreLocation location, Metadata metadata, Func<Stream, Task> savingFunc)
        {
            return SaveDataInternal(location, metadata, savingFunc, false);
        }

        public async Task SaveMetadata(StoreLocation location, Metadata metadata)
        {
            var blob = GetBlockBlob(location);
            try
            {
                await blob.FetchAttributesAsync().ConfigureAwait(false);
            }
            catch (StorageException e)
            {
                // No metadata to update in this case...
                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    return;
                }

                throw e.Wrap();
            }

            foreach(var m in metadata)
            {
                blob.Metadata[m.Key] = m.Value;
            }

            await blob.SetMetadataAsync().ConfigureAwait(false);
        }

        public Task<bool> TryOptimisticWrite(StoreLocation location, Metadata metadata, Func<Stream, Task> savingFunc)
        {
            return SaveDataInternal(location, metadata, savingFunc, true);
        }

        public async Task<IDisposable> Lock(StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            var l = await LockInternal(blob).ConfigureAwait(false);
            return l == null ? null : l.Item1;
        }

        public Task RunOnce(StoreLocation location, Func<Task> action) { return RunOnce(location, action, TimeSpan.FromSeconds(5)); }
        private async Task RunOnce(StoreLocation location, Func<Task> action, TimeSpan pollingFrequency)
        {
            var blob = GetBlockBlob(location);
            // blob.Exists has the side effect of calling blob.FetchAttributes, which populates the metadata collection
            while (!(await blob.ExistsAsync().ConfigureAwait(false)) || !blob.Metadata.ContainsKey("progress") || blob.Metadata["progress"] != "done")
            {
                var lease = await LockInternal(blob).ConfigureAwait(false);
                if (lease != null)
                {
                    using (var arl = lease.Item1)
                    {
                        // Once we have the lock make sure we are not done!
                        if (!blob.Metadata.ContainsKey("progress") || blob.Metadata["progress"] != "done")
                        {
                            await action().ConfigureAwait(false);
                            blob.Metadata["progress"] = "done";
                            await blob.SetMetadataAsync(AccessCondition.GenerateLeaseCondition(lease.Item2), null, null).ConfigureAwait(false);
                        }
                    }
                }
                else
                {
                    await Task.Delay(pollingFrequency).ConfigureAwait(false);
                }
            }
        }

        public IObservable<bool> RunEvery(StoreLocation location, TimeSpan interval, Action<Exception> unhandledExceptions = null)
        {
            return Observable.Create<bool>(async (obs, ct) =>
            {
                var blob = GetBlockBlob(location);

                while (!ct.IsCancellationRequested)
                {
                    // Don't allow you to throw to get out of the loop...
                    try
                    {
                        var lastPerformed = DateTimeOffset.MinValue;
                        var lease = await LockInternal(blob).ConfigureAwait(false);
                        if (lease != null)
                        {
                            using (var arl = lease.Item1)
                            {
                                await blob.FetchAttributesAsync().ConfigureAwait(false);
                                if (blob.Metadata.ContainsKey("lastPerformed"))
                                {
                                    DateTimeOffset.TryParseExact(blob.Metadata["lastPerformed"], "R", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out lastPerformed);
                                }
                                if (DateTimeOffset.UtcNow >= lastPerformed + interval)
                                {
                                    obs.OnNext(true);
                                    lastPerformed = DateTimeOffset.UtcNow;
                                    blob.Metadata["lastPerformed"] = lastPerformed.ToString("R", CultureInfo.InvariantCulture);
                                    await blob.SetMetadataAsync(AccessCondition.GenerateLeaseCondition(lease.Item2), null, null).ConfigureAwait(false);
                                }
                            }
                        }
                        var timeLeft = (lastPerformed + interval) - DateTimeOffset.UtcNow;
                        var minimum = TimeSpan.FromSeconds(5); // so we're not polling the leased blob too fast
                        await Task.Delay(timeLeft > minimum ? timeLeft : minimum).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        if (unhandledExceptions != null)
                        {
                            unhandledExceptions(e);
                        }
                            
                        LeoTrace.WriteLine("Error on lock loop: " + e.Message);
                    }
                }
            });
        }

        public async Task<Metadata> GetMetadata(StoreLocation location, string snapshot = null)
        {
            var blob = GetBlockBlob(location, snapshot);
            try
            {
                await blob.FetchAttributesAsync().ConfigureAwait(false);
                LeoTrace.WriteLine("Got Metadata: " + blob.Name);
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    return null;
                }

                throw e.Wrap();
            }

            var metadata = GetActualMetadata(blob);
            if (metadata.ContainsKey(_deletedKey))
            {
                metadata = null;
            }
            return metadata;
        }

        public async Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null)
        {
            var blob = GetBlockBlob(location, snapshot);

            // Get metadata first - this record might be deleted so quicker to test this than to download whole record...
            try
            {
                LeoTrace.WriteLine("Downloading blob metadata: " + blob.Name);
                await blob.FetchAttributesAsync().ConfigureAwait(false);
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    return null;
                }

                throw e.Wrap();
            }

            // If deleted then return null...
            var metadata = GetActualMetadata(blob);
            if (metadata.ContainsKey(_deletedKey))
            {
                return null;
            }

            return new DataWithMetadata(new AzureReadBlockBlobStream(blob), metadata);
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

        public async Task SoftDelete(StoreLocation location)
        {
            // In Azure we cannot delete the blob as this will loose the snapshots
            // Instead we will just add some metadata
            var blob = GetBlockBlob(location);
            try
            {
                await blob.FetchAttributesAsync().ConfigureAwait(false);
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404)
                {
                    return;
                }

                throw e.Wrap();
            }

            blob.Metadata[_deletedKey] = DateTime.UtcNow.Ticks.ToString();
            await blob.SetMetadataAsync().ConfigureAwait(false);
            LeoTrace.WriteLine("Soft deleted (2 calls): " + blob.Name);
        }

        public Task PermanentDelete(StoreLocation location)
        {
            var blob = GetBlockBlob(location);
            LeoTrace.WriteLine("Deleted blob: " + blob.Name);
            return blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, null, null, null);
        }

        public Task CreateContainerIfNotExists(string container)
        {
            container = SafeContainerName(container);
            LeoTrace.WriteLine("Trying to create container: " + container);

            var c = _blobStorage.GetContainerReference(container);
            return c.CreateIfNotExistsAsync();
        }

        public Task PermanentDeleteContainer(string container)
        {
            container = SafeContainerName(container);
            LeoTrace.WriteLine("Trying to delete container: " + container);

            var c = _blobStorage.GetContainerReference(container);
            return c.DeleteIfExistsAsync();
        }

        private async Task ListBlobs(IObserver<ICloudBlob> observer, CloudBlobContainer container, string prefix, BlobListingDetails options, CancellationToken ct)
        {
            BlobContinuationToken token = new BlobContinuationToken();

            try
            {
                do
                {
                    var segment = await container.ListBlobsSegmentedAsync(prefix, true, options, null, token, null, null, ct).ConfigureAwait(false);
                    LeoTrace.WriteLine("Listed blob segment for prefix: " + prefix);

                    foreach (var blob in segment.Results.OfType<ICloudBlob>())
                    {
                        observer.OnNext(blob);
                    }

                    token = segment.ContinuationToken;
                }
                while (token != null && !ct.IsCancellationRequested);

                ct.ThrowIfCancellationRequested(); // We should throw this to be consistent...
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
                    observer.OnError(e.Wrap());
                }
            }
            catch (Exception e)
            {
                observer.OnError(e);
            }
        }

        private async Task<Tuple<IDisposable, string>> LockInternal(ICloudBlob blob)
        {
            string leaseId;

            try
            {
                leaseId = await blob.AcquireLeaseAsync(TimeSpan.FromMinutes(1), null).ConfigureAwait(false);
                LeoTrace.WriteLine("Leased Blob: " + blob.Name);
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
                    throw e.Wrap();
                }
            }

            // May not have had a blob pushed...
            if (leaseId == null)
            {
                try
                {
                    using (var stream = new MemoryStream(new byte[1]))
                    {
                        try
                        {
                            await blob.UploadFromStreamAsync(stream).ConfigureAwait(false);
                        }
                        catch (StorageException e) { }  // Just eat storage exceptions at this point... something was created obviously
                    }
                    leaseId = await blob.AcquireLeaseAsync(TimeSpan.FromMinutes(1), null).ConfigureAwait(false);
                    LeoTrace.WriteLine("Created new blob and lease (2 calls): " + blob.Name);
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
                        throw e.Wrap();
                    }
                }
            }

            var condition = AccessCondition.GenerateLeaseCondition(leaseId);
            var release = Disposable.Create(() =>
            {
                try
                {
                    blob.ReleaseLease(condition);
                }
                catch (Exception e)
                {
                    LeoTrace.WriteLine("Release failed: " + e.Message);
                }
            });

            // Every 30 secs keep the lock renewed
            IDisposable keepAlive = null;
            keepAlive = Observable
                .Interval(TimeSpan.FromSeconds(30))
                .Subscribe(
                    _ =>
                    {
                        try
                        {
                            blob.RenewLease(condition);
                            LeoTrace.WriteLine("Renewed Lease: " + blob.Name);
                        }
                        catch (Exception e)
                        {
                            LeoTrace.WriteLine("Failed to renew lease: " + e.Message);

                            if (keepAlive != null)
                            {
                                keepAlive.Dispose();
                            }
                        }
                    },
                    () => { release.Dispose(); }
                );


            return Tuple.Create((IDisposable)(new CompositeDisposable(keepAlive, release)), leaseId);
        }

        private async Task<bool> SaveDataInternal(StoreLocation location, Metadata metadata, Func<Stream, Task> savingFunc, bool isOptimistic)
        {
            try
            {
                var blob = GetBlockBlob(location);

                // If the ETag value is empty then the store value must not exist yet...
                var condition = isOptimistic ? 
                    (metadata == null || string.IsNullOrEmpty(metadata.ETag) ? AccessCondition.GenerateIfNoneMatchCondition("*") : AccessCondition.GenerateIfMatchCondition(metadata.ETag)) 
                    : null;

                // Copy the metadata across
                blob.Metadata.Clear();
                if (metadata != null)
                {
                    foreach (var m in metadata)
                    {
                        blob.Metadata[m.Key] = m.Value;
                    }
                }

                using (var stream = new AzureWriteBlockBlobStream(blob, condition))
                {
                    await savingFunc(stream).ConfigureAwait(false);
                    await stream.Complete().ConfigureAwait(false);
                }

                // Create a snapshot straight away on azure
                // Note: this shouldnt matter for cost as any blocks that are the same do not cost extra
                if (_enableSnapshots)
                {
                    await blob.CreateSnapshotAsync().ConfigureAwait(false);
                    LeoTrace.WriteLine("Created Snapshot: " + blob.Name);
                }
            }
            catch (StorageException exc)
            {
                if (isOptimistic)
                {
                    // First condition occurrs when the eTags do not match
                    // Second condition when we specified no eTag (ie must be new blob)
                    if (exc.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed  
                        || (exc.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict && exc.RequestInformation.ExtendedErrorInformation.ErrorCode == "BlobAlreadyExists"))
                    {
                        return false;
                    }
                }

                if(exc.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                {
                    throw new LockException("The underlying storage is currently locked for save");
                }

                throw exc.Wrap();
            }

            return true;
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

            if (!metadata.ContainsKey(MetadataConstants.ContentTypeMetadataKey))
            {
                metadata[MetadataConstants.ContentTypeMetadataKey] = blob.Properties.ContentType;
            }

            metadata.ETag = blob.Properties.ETag;

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
