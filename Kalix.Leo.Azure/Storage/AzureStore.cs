using Kalix.Leo.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage
{
    public class AzureStore : IOptimisticStore
    {
        private const string IdExtension = ".id";
        private const string DefaultDeletedKey = "leodeleted";
        private const string InternalSnapshotKey = "leoazuresnapshot";
        private const string StoreVersionKey = "leoazureversion";
        private const string StoreVersionValue = "2.0";

        private readonly CloudBlobClient _blobStorage;
        private readonly string _deletedKey;
        private readonly string _containerPrefix;
        private readonly bool _enableSnapshots;

        /// <summary>
        /// Constructor for a store backed by Azure
        /// </summary>
        /// <param name="blobStorage">The storage account that backs this store</param>
        /// <param name="enableSnapshots">Whether any save on this store should create snapshots</param>
        /// <param name="deletedKey">The metadata key to check if a store item is soft deleted</param>
        /// <param name="containerPrefix">Use this to namespace your containers if required</param>
        public AzureStore(CloudBlobClient blobStorage, bool enableSnapshots, string deletedKey = null, string containerPrefix = null)
        {
            _blobStorage = blobStorage;
            _enableSnapshots = enableSnapshots;
            _deletedKey = deletedKey ?? DefaultDeletedKey;
            _containerPrefix = containerPrefix;
        }

        public async Task<Metadata> SaveData(StoreLocation location, Metadata metadata, Func<IWriteAsyncStream, Task<long?>> savingFunc, CancellationToken token)
        {
            var result = await SaveDataInternal(location, metadata, savingFunc, token, false).ConfigureAwait(false);
            return result.Metadata;
        }

        public async Task<Metadata> SaveMetadata(StoreLocation location, Metadata metadata)
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
                    return null;
                }

                throw e.Wrap(blob.Name);
            }

            foreach(var m in metadata)
            {
                blob.Metadata[m.Key] = m.Value;
            }

            await blob.SetMetadataAsync().ConfigureAwait(false);
            return await GetActualMetadata(blob).ConfigureAwait(false);
        }

        public Task<OptimisticStoreWriteResult> TryOptimisticWrite(StoreLocation location, Metadata metadata, Func<IWriteAsyncStream, Task<long?>> savingFunc, CancellationToken token)
        {
            return SaveDataInternal(location, metadata, savingFunc, token, true);
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

        public IAsyncEnumerable<bool> RunEvery(StoreLocation location, TimeSpan interval, Action<Exception> unhandledExceptions = null)
        {
            return AsyncEnumerableEx.Create<bool>(async (y) =>
            {
                var blob = GetBlockBlob(location);

                while (!y.CancellationToken.IsCancellationRequested)
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
                                await blob.FetchAttributesAsync(y.CancellationToken).ConfigureAwait(false);
                                if (blob.Metadata.ContainsKey("lastPerformed"))
                                {
                                    DateTimeOffset.TryParseExact(blob.Metadata["lastPerformed"], "R", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out lastPerformed);
                                }
                                if (DateTimeOffset.UtcNow >= lastPerformed + interval)
                                {
                                    await y.YieldReturn(true).ConfigureAwait(false);
                                    lastPerformed = DateTimeOffset.UtcNow;
                                    blob.Metadata["lastPerformed"] = lastPerformed.ToString("R", CultureInfo.InvariantCulture);
                                    await blob.SetMetadataAsync(AccessCondition.GenerateLeaseCondition(lease.Item2), null, null, y.CancellationToken).ConfigureAwait(false);
                                }
                            }
                        }
                        var timeLeft = (lastPerformed + interval) - DateTimeOffset.UtcNow;
                        var minimum = TimeSpan.FromSeconds(5); // so we're not polling the leased blob too fast
                        await Task.Delay(timeLeft > minimum ? timeLeft : minimum, y.CancellationToken).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException) { throw; }
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

                throw e.Wrap(blob.Name);
            }

            var metadata = await GetActualMetadata(blob).ConfigureAwait(false);
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

                throw e.Wrap(blob.Name);
            }

            // If deleted then return null...
            var metadata = await GetActualMetadata(blob).ConfigureAwait(false);
            if (metadata.ContainsKey(_deletedKey))
            {
                return null;
            }
            
            // Older versions need to check the block list before loading...
            var needsToUseBlockList = !blob.Metadata.ContainsKey(StoreVersionKey);
            return new DataWithMetadata(new AzureReadBlockBlobStream(blob, needsToUseBlockList), metadata);
        }

        public IAsyncEnumerable<Snapshot> FindSnapshots(StoreLocation location)
        {
            if (!_enableSnapshots) { return AsyncEnumerable.Empty<Snapshot>(); }

            var blob = GetBlockBlob(location);
            return ListBlobs(blob.Container, blob.Name, BlobListingDetails.Snapshots | BlobListingDetails.Metadata)
                .Where(b => b.IsSnapshot && b.Uri == blob.Uri && b.SnapshotTime.HasValue)
                .Select(async b => new Snapshot
                {
                    Id = b.SnapshotTime.Value.UtcTicks.ToString(CultureInfo.InvariantCulture),
                    Metadata = await GetActualMetadata(b).ConfigureAwait(false)
                })
                .Unwrap();
        }

        public IAsyncEnumerable<LocationWithMetadata> FindFiles(string container, string prefix = null)
        {
            var c = _blobStorage.GetContainerReference(SafeContainerName(container));
            return ListBlobs(c, prefix, BlobListingDetails.Metadata)
                .Where(b => !b.Metadata.ContainsKey(_deletedKey)) // Do not include blobs which are soft deleted
                .Select(async b =>
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
                    return new LocationWithMetadata(loc, await GetActualMetadata(b).ConfigureAwait(false));
                })
                .Unwrap();
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

                throw e.Wrap(blob.Name);
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

        private IAsyncEnumerable<ICloudBlob> ListBlobs(CloudBlobContainer container, string prefix, BlobListingDetails options)
        {
            return AsyncEnumerableEx.Create<ICloudBlob>(async (y) =>
            {
                BlobContinuationToken token = new BlobContinuationToken();

                try
                {
                    do
                    {
                        var segment = await container.ListBlobsSegmentedAsync(prefix, true, options, null, token, null, null, y.CancellationToken).ConfigureAwait(false);
                        LeoTrace.WriteLine("Listed blob segment for prefix: " + prefix);

                        foreach (var blob in segment.Results.OfType<ICloudBlob>())
                        {
                            await y.YieldReturn(blob).ConfigureAwait(false);
                        }

                        token = segment.ContinuationToken;
                    }
                    while (token != null && !y.CancellationToken.IsCancellationRequested);
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode != 404)
                    {
                        throw e.Wrap(container.Name + "_" + (prefix ?? string.Empty) + "*");
                    }
                }
            });
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
                    throw e.Wrap(blob.Name);
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
                        catch (StorageException) { }  // Just eat storage exceptions at this point... something was created obviously
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
                        throw e.Wrap(blob.Name);
                    }
                }
            }

            var condition = AccessCondition.GenerateLeaseCondition(leaseId);

            // Every 30 secs keep the lock renewed
            var keepAlive = AsyncEnumerableEx.CreateTimer(TimeSpan.FromSeconds(30))
                .Select(t =>
                {
                    LeoTrace.WriteLine("Renewed Lease: " + blob.Name);
                    return blob.RenewLeaseAsync(condition);
                })
                .Unwrap()
                .TakeUntilDisposed(null, t =>
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


            return Tuple.Create((IDisposable)keepAlive, leaseId);
        }

        private async Task<OptimisticStoreWriteResult> SaveDataInternal(StoreLocation location, Metadata metadata, Func<IWriteAsyncStream, Task<long?>> savingFunc, CancellationToken token, bool isOptimistic)
        {
            var blob = GetBlockBlob(location);

            var result = new OptimisticStoreWriteResult() { Result = true };
            try
            {
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

                // Always store the version - We use this to do more efficient things on read
                blob.Metadata[StoreVersionKey] = StoreVersionValue;
                
                long? length;
                using (var stream = new AzureWriteBlockBlobStream(blob, condition))
                {
                    length = await savingFunc(stream).ConfigureAwait(false);
                    await stream.Complete(token).ConfigureAwait(false);
                }

                if (length.HasValue && (metadata == null || !metadata.ContentLength.HasValue))
                {
                    blob.Metadata[MetadataConstants.ContentLengthMetadataKey] = length.Value.ToString(CultureInfo.InvariantCulture);

                    // Save the length straight away before the snapshot...
                    await blob.SetMetadataAsync(token).ConfigureAwait(false);
                }

                // Create a snapshot straight away on azure
                // Note: this shouldnt matter for cost as any blocks that are the same do not cost extra
                if (_enableSnapshots)
                {
                    var snapshotBlob = await blob.CreateSnapshotAsync(token).ConfigureAwait(false);
                    var snapshot = snapshotBlob.SnapshotTime.Value.UtcTicks.ToString(CultureInfo.InvariantCulture);

                    // Save the snapshot back to original blob...
                    blob.Metadata[InternalSnapshotKey] = snapshot;
                    await blob.SetMetadataAsync(token).ConfigureAwait(false);

                    LeoTrace.WriteLine("Created Snapshot: " + blob.Name);
                }

                result.Metadata = await GetActualMetadata(blob).ConfigureAwait(false);
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
                        result.Result = false;
                    }
                }
                else
                {
                    if (exc.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict
                        || exc.RequestInformation.ExtendedErrorInformation.ErrorCode == "LeaseIdMissing")
                    {
                        throw new LockException("The underlying storage is currently locked for save");
                    }

                    throw exc.Wrap(blob.Name);
                }
            }

            return result;
        }

        private async Task<Metadata> GetActualMetadata(ICloudBlob blob)
        {
            var metadata = new Metadata(blob.Metadata);

            if (blob.Properties.LastModified.HasValue || blob.SnapshotTime.HasValue)
            {
                metadata.StoredLastModified = blob.SnapshotTime.HasValue ? blob.SnapshotTime.Value.UtcDateTime : blob.Properties.LastModified.Value.UtcDateTime;
                if(!metadata.LastModified.HasValue)
                {
                    metadata.LastModified = metadata.StoredLastModified;
                }
            }

            metadata.StoredContentLength = blob.Properties.Length;
            metadata.StoredContentType = blob.Properties.ContentType;
            metadata.ETag = blob.Properties.ETag;

            // Remove the snapshot key at this point if we have it
            if(metadata.ContainsKey(InternalSnapshotKey))
            {
                metadata.Remove(InternalSnapshotKey);
            }

            // Remove the store key as well...
            if(metadata.ContainsKey(StoreVersionKey))
            {
                metadata.Remove(StoreVersionKey);
            }

            if (_enableSnapshots)
            {
                if (blob.IsSnapshot)
                {
                    metadata.Snapshot = blob.SnapshotTime.Value.UtcTicks.ToString(CultureInfo.InvariantCulture);
                }
                else if (blob.Metadata.ContainsKey(InternalSnapshotKey))
                {
                    // Try and use our save snapshot instead of making more calls...
                    metadata.Snapshot = blob.Metadata[InternalSnapshotKey];
                }
                else
                {
                    // Try and find last snapshot
                    // Unfortunately we need to make a request since this information isn't on the actual blob that we are working with...
                    var snapBlob = await ListBlobs(blob.Container, blob.Name, BlobListingDetails.Snapshots | BlobListingDetails.Metadata)
                        .Where(b => b.IsSnapshot && b.Uri == blob.Uri && b.SnapshotTime.HasValue)
                        .Scan((a, b) => a.SnapshotTime.Value > b.SnapshotTime.Value ? a : b)
                        .LastOrDefault()
                        .ConfigureAwait(false);

                    metadata.Snapshot = snapBlob == null ? null : snapBlob.SnapshotTime.Value.UtcTicks.ToString(CultureInfo.InvariantCulture);
                }
            }

            return metadata;
        }

        private CloudBlockBlob GetBlockBlob(StoreLocation location, string snapshot = null)
        {
            DateTime? snapshotDate = null;
            if (snapshot != null)
            {
                snapshotDate = new DateTime(long.Parse(snapshot, CultureInfo.InvariantCulture));
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
            if(_containerPrefix != null)
            {
                containerName = _containerPrefix + containerName;
            }

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
