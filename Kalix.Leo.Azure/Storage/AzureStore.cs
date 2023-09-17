using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage;

public class AzureStore : IOptimisticStore
{
    private const string IdExtension = ".id";
    private const string DefaultDeletedKey = "leodeleted";
    private const string InternalSnapshotKey = "leoazuresnapshot";
    private const string StoreVersionKey = "leoazureversion";
    private const string StoreVersionValue = "2.0";

    private const string ProgressMetadataKey = "progress";
    private const string ProgressDoneValue = "done";

    private static readonly TimeSpan LeaseDuration = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan LeaseRenewDuration = TimeSpan.FromSeconds(30);

    private readonly BlobServiceClient _blobStorage;
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
    public AzureStore(BlobServiceClient blobStorage, bool enableSnapshots, string deletedKey = null, string containerPrefix = null)
    {
        _blobStorage = blobStorage;
        _enableSnapshots = enableSnapshots;
        _deletedKey = deletedKey ?? DefaultDeletedKey;
        _containerPrefix = containerPrefix;
    }

    public async Task<Metadata> SaveData(StoreLocation location, Metadata metadata, UpdateAuditInfo audit, Func<PipeWriter, Task<long?>> savingFunc, CancellationToken token)
    {
        var result = await SaveDataInternal(location, metadata, audit, savingFunc, false, token);
        return result.Metadata;
    }

    public async Task<Metadata> SaveMetadata(StoreLocation location, Metadata metadata)
    {
        var blob = GetBlockBlob(location);
        var props = await GetBlobProperties(blob);
        if (props == null) { return null; }

        foreach (var m in metadata)
        {
            // Do not override audit information!
            if (m.Key != MetadataConstants.AuditMetadataKey)
            {
                props.Metadata[m.Key] = AzureStoreMetadataEncoder.EncodeMetadata(m.Value);
            }
        }

        await blob.SetMetadataAsync(props.Metadata);
        return await GetActualMetadata(blob, props, null);
    }

    public Task<OptimisticStoreWriteResult> TryOptimisticWrite(StoreLocation location, Metadata metadata, UpdateAuditInfo audit, Func<PipeWriter, Task<long?>> savingFunc, CancellationToken token)
    {
        return SaveDataInternal(location, metadata, audit, savingFunc, true, token);
    }

    public async Task<(IAsyncDisposable CancelDispose, Task Task)> Lock(StoreLocation location)
    {
        var blob = GetBlockBlob(location);
        var l = await LockInternal(blob);
        return (l?.Item1, l?.Item3);
    }

    public Task RunOnce(StoreLocation location, Func<Task> action) { return RunOnce(location, action, TimeSpan.FromSeconds(5)); }
    private async Task RunOnce(StoreLocation location, Func<Task> action, TimeSpan pollingFrequency)
    {
        var blob = GetBlockBlob(location);
        // blob.Exists has the side effect of calling blob.FetchAttributes, which populates the metadata collection
        while (true)
        {
            BlobProperties props;
            try
            {
                props = await GetBlobProperties(blob);
                if (props != null && props.Metadata.ContainsKey(ProgressMetadataKey) && props.Metadata[ProgressMetadataKey] == ProgressDoneValue)
                {
                    break;
                }
            }
            catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobNotFound) { }

            var lease = await LockInternal(blob);
            if (lease != null)
            {
                await using var arl = lease.Item1;

                // Once we have the lock make sure we are not done!
                props = await GetBlobProperties(blob);
                if (props.Metadata.ContainsKey(ProgressMetadataKey) && props.Metadata[ProgressMetadataKey] == ProgressDoneValue)
                {
                    break;
                }

                await action();
                props.Metadata[ProgressMetadataKey] = ProgressDoneValue;
                await blob.SetMetadataAsync(props.Metadata, new BlobRequestConditions { LeaseId = lease.Item2 });
            }
            else
            {
                await Task.Delay(pollingFrequency);
            }
        }
    }

    public async IAsyncEnumerable<bool> RunEvery(StoreLocation location, TimeSpan interval, Action<Exception> unhandledExceptions = null, [EnumeratorCancellation] CancellationToken token = default)
    {
        var blob = GetBlockBlob(location);

        var minimum = TimeSpan.FromSeconds(5); // so we're not polling the leased blob too fast
        while (!token.IsCancellationRequested)
        {
            var timeLeft = TimeSpan.FromSeconds(0);

            // Don't allow you to throw to get out of the loop...
            bool canExecute = false;
            try
            {
                var lastPerformed = DateTimeOffset.MinValue;
                var lease = await LockInternal(blob);
                if (lease != null)
                {
                    await using var arl = lease.Item1;

                    var props = await GetBlobProperties(blob, token);
                    if (props.Metadata.ContainsKey("lastPerformed"))
                    {
                        DateTimeOffset.TryParseExact(props.Metadata["lastPerformed"], "R", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out lastPerformed);
                    }
                    if (DateTimeOffset.UtcNow >= lastPerformed + interval)
                    {
                        lastPerformed = DateTimeOffset.UtcNow;
                        props.Metadata["lastPerformed"] = lastPerformed.ToString("R", CultureInfo.InvariantCulture);
                        await blob.SetMetadataAsync(props.Metadata, new BlobRequestConditions { LeaseId = lease.Item2 }, token);
                        canExecute = true;
                    }
                }
                timeLeft = (lastPerformed + interval) - DateTimeOffset.UtcNow;
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception e)
            {
                unhandledExceptions?.Invoke(e);
                LeoTrace.WriteLine("Error on lock loop: " + e.Message);
            }

            if (canExecute)
            {
                yield return true;
            }

            // Do this outside the exception to prevent it going out of control
            await Task.Delay(timeLeft > minimum ? timeLeft : minimum, token);
        }
    }

    public async Task<Metadata> GetMetadata(StoreLocation location, string snapshot = null)
    {
        var blob = GetBlockBlob(location, snapshot);
        var metadata = await GetBlobMetadata(blob, snapshot);

        return metadata == null || metadata.ContainsKey(_deletedKey) ? null : metadata;
    }

    public async Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null)
    {
        var blob = GetBlockBlob(location, snapshot);

        // Get metadata first - this record might be deleted so quicker to test this than to download whole record...
        // If deleted then return null...
        var props = await GetBlobProperties(blob);
        if (props == null || props.Metadata.ContainsKey(_deletedKey))
        {
            return null;
        }

        // Older versions need to check the block list before loading...
        var needsToUseBlockList = !props.Metadata.ContainsKey(StoreVersionKey);
        var metadata = await GetActualMetadata(blob, props, snapshot);

        var readStream = new AzureReadBlockBlobStream(blob, props.ContentLength, needsToUseBlockList);
        return new DataWithMetadata(PipeReader.Create(readStream), metadata);
    }

    public IAsyncEnumerable<Snapshot> FindSnapshots(StoreLocation location)
    {
        if (!_enableSnapshots) { return AsyncEnumerable.Empty<Snapshot>(); }

        var blob = GetBlockBlob(location);
        var container = _blobStorage.GetBlobContainerClient(blob.BlobContainerName);
        return ListBlobs(container, blob.Name, BlobTraits.Metadata, BlobStates.Snapshots)
            .Where(b => !string.IsNullOrWhiteSpace(b.Snapshot) && b.Name == blob.Name)
            .Select(b => new Snapshot
            {
                Id = b.Snapshot,
                Metadata = GetActualMetadata(b)
            });
    }

    public async IAsyncEnumerable<LocationWithMetadata> FindFiles(string container, string prefix = null)
    {
        var c = _blobStorage.GetBlobContainerClient(SafeContainerName(container));
        var blobs = ListBlobs(c, prefix, BlobTraits.Metadata, BlobStates.None)
            .Where(b => !b.Metadata.ContainsKey(_deletedKey)) // Do not include blobs which are soft deleted
            .Select(b =>
            {
                long? id = null;
                string path = b.Name;
                if (path.EndsWith(IdExtension))
                {
                    if (long.TryParse(Path.GetFileNameWithoutExtension(path), out var tempId))
                    {
                        id = tempId;
                        path = Path.GetDirectoryName(path);
                    }
                }

                var loc = new StoreLocation(container, path, id);
                return new LocationWithMetadata(loc, GetActualMetadata(b));
            });

        // Wierd setup here so we can catch the error if no container and just return empty
        await using var en = blobs.GetAsyncEnumerator();
        while (true)
        {
            try
            {
                if (!await en.MoveNextAsync()) { yield break; }
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "ContainerNotFound")
            {
                yield break;
            }

            yield return en.Current;
        }
    }

    public async Task SoftDelete(StoreLocation location, UpdateAuditInfo audit)
    {
        // In Azure we cannot delete the blob as this will loose the snapshots
        // Instead we will just add some metadata
        var blob = GetBlockBlob(location);

        var props = await GetBlobProperties(blob);
        var meta = await GetActualMetadata(blob, props, null);

        // If already deleted don't worry about it!
        if (meta == null || meta.ContainsKey(_deletedKey)) { return; }

        var fullInfo = TransformAuditInformation(meta, audit);
        meta.Audit = fullInfo;

        props.Metadata[MetadataConstants.AuditMetadataKey] = AzureStoreMetadataEncoder.EncodeMetadata(meta[MetadataConstants.AuditMetadataKey]);
        props.Metadata[_deletedKey] = DateTime.UtcNow.Ticks.ToString();
        await blob.SetMetadataAsync(props.Metadata);
        LeoTrace.WriteLine("Soft deleted (2 calls): " + blob.Name);
    }

    public Task PermanentDelete(StoreLocation location)
    {
        var blob = GetBlockBlob(location);
        LeoTrace.WriteLine("Deleted blob: " + blob.Name);
        return blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
    }

    public Task CreateContainerIfNotExists(string container)
    {
        container = SafeContainerName(container);
        LeoTrace.WriteLine("Trying to create container: " + container);

        var c = _blobStorage.GetBlobContainerClient(container);
        return c.CreateIfNotExistsAsync();
    }

    public Task PermanentDeleteContainer(string container)
    {
        container = SafeContainerName(container);
        LeoTrace.WriteLine("Trying to delete container: " + container);

        var c = _blobStorage.GetBlobContainerClient(container);
        return c.DeleteIfExistsAsync();
    }

    private async Task<Metadata> GetBlobMetadata(BlockBlobClient blob, string snapshot)
    {
        LeoTrace.WriteLine("Downloading blob metadata: " + blob.Name);

        try
        {
            var props = await blob.GetPropertiesAsync();
            return await GetActualMetadata(blob, props, snapshot);
        }
        catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobNotFound)
        {
            return null;
        }
    }

    private async Task<OptimisticStoreWriteResult> SaveDataInternal(StoreLocation location, Metadata metadata, UpdateAuditInfo audit, Func<PipeWriter, Task<long?>> savingFunc, bool isOptimistic, CancellationToken token)
    {
        var blob = GetBlockBlob(location);

        // We always want to save the new audit information when saving!
        var props = await GetBlobProperties(blob, token);
        var currentMetadata = await GetActualMetadata(blob, props, null);
        var auditInfo = TransformAuditInformation(currentMetadata, audit);
        metadata ??= new Metadata();
        metadata.Audit = auditInfo;

        var result = new OptimisticStoreWriteResult() { Result = true };
        try
        {
            // If the ETag value is empty then the store value must not exist yet...
            var condition = isOptimistic ? 
                (string.IsNullOrEmpty(metadata.ETag) ? new BlobRequestConditions { IfNoneMatch = ETag.All } : new BlobRequestConditions { IfMatch = new ETag(metadata.ETag) }) 
                : null;

            // Copy the metadata across
            var newMeta = new Dictionary<string, string>();
            foreach (var m in metadata)
            {
                newMeta[m.Key] = AzureStoreMetadataEncoder.EncodeMetadata(m.Value);
            }

            // Always store the version - We use this to do more efficient things on read
            newMeta[StoreVersionKey] = StoreVersionValue;
                
            long? length;
            using (var stream = new AzureWriteBlockBlobStream(blob, condition, newMeta))
            {
                var writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
                length = await savingFunc(writer);
                await writer.CompleteAsync();
                await stream.Complete(token);
            }

            if (length.HasValue && (metadata == null || !metadata.ContentLength.HasValue))
            {
                newMeta[MetadataConstants.ContentLengthMetadataKey] = length.Value.ToString(CultureInfo.InvariantCulture);

                // Save the length straight away before the snapshot...
                await blob.SetMetadataAsync(newMeta, cancellationToken: token);
            }

            // Create a snapshot straight away on azure
            // Note: this shouldnt matter for cost as any blocks that are the same do not cost extra
            if (_enableSnapshots)
            {
                var snapshotBlob = await blob.CreateSnapshotAsync(cancellationToken: token);

                // Save the snapshot back to original blob...
                newMeta[InternalSnapshotKey] = snapshotBlob.Value.Snapshot;
                await blob.SetMetadataAsync(newMeta, cancellationToken: token);

                LeoTrace.WriteLine("Created Snapshot: " + blob.Name);
            }

            var newProps = await blob.GetPropertiesAsync(cancellationToken: token);
            result.Metadata = await GetActualMetadata(blob, newProps, null);
        }
        catch (RequestFailedException e)
        {
            if (isOptimistic)
            {
                // First condition occurrs when the eTags do not match
                // Second condition when we specified no eTag (ie must be new blob)
                if (e.Status == (int)HttpStatusCode.PreconditionFailed
                    || (e.Status == (int)HttpStatusCode.Conflict && e.ErrorCode == BlobErrorCode.BlobAlreadyExists))
                {
                    result.Result = false;
                }
                else
                {
                    // Might have been a different error?
                    throw;
                }
            }
            else
            {
                if (e.Status == (int)HttpStatusCode.Conflict || e.ErrorCode == BlobErrorCode.LeaseIdMissing)
                {
                    throw new LockException("The underlying storage is currently locked for save");
                }

                // Might have been a different error?
                throw;
            }
        }

        return result;
    }

    private Metadata GetActualMetadata(BlobItem item)
    {
        // Pass all custom metadata through the converter
        var convertedMeta = item.Metadata.ToDictionary(k => k.Key, k => AzureStoreMetadataEncoder.DecodeMetadata(k.Value));
        var metadata = new Metadata(convertedMeta)
        {
            StoredLastModified = item.Properties.LastModified.Value.UtcDateTime,
            StoredContentLength = item.Properties.ContentLength,
            StoredContentType = item.Properties.ContentType,
            ETag = item.Properties.ETag.ToString()
        };

        if (!metadata.LastModified.HasValue)
        {
            metadata.LastModified = metadata.StoredLastModified;
        }

        // Remove the snapshot key at this point if we have it
        metadata.Remove(InternalSnapshotKey);

        // Remove the store key as well...
        metadata.Remove(StoreVersionKey);

        if (_enableSnapshots && !string.IsNullOrEmpty(item.Snapshot))
        {
            metadata.Snapshot = item.Snapshot;
        }

        return metadata;
    }

    private async ValueTask<Metadata> GetActualMetadata(BlockBlobClient blob, BlobProperties props, string snapshot)
    {
        if (props == null) { return null; }

        // Pass all custom metadata through the converter
        var convertedMeta = props.Metadata.ToDictionary(k => k.Key, k => AzureStoreMetadataEncoder.DecodeMetadata(k.Value));
        var metadata = new Metadata(convertedMeta)
        {
            StoredLastModified = props.LastModified.UtcDateTime,
            StoredContentLength = props.ContentLength,
            StoredContentType = props.ContentType,
            ETag = props.ETag.ToString()
        };

        if (!metadata.LastModified.HasValue)
        {
            metadata.LastModified = metadata.StoredLastModified;
        }

        // Remove the snapshot key at this point if we have it
        metadata.Remove(InternalSnapshotKey);

        // Remove the store key as well...
        metadata.Remove(StoreVersionKey);

        if (_enableSnapshots)
        {
            if (!string.IsNullOrEmpty(snapshot))
            {
                metadata.Snapshot = snapshot;
            }
            else if (props.Metadata.ContainsKey(InternalSnapshotKey))
            {
                // Try and use our save snapshot instead of making more calls...
                var date = props.Metadata[InternalSnapshotKey];
                if (long.TryParse(date, out var ticks))
                {
                    // This is in old format, convert back to snapshot format date
                    date = new DateTime(ticks, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
                }
                metadata.Snapshot = date;
            }
            else
            {
                // Try and find last snapshot
                // Unfortunately we need to make a request since this information isn't on the actual blob that we are working with...
                var container = _blobStorage.GetBlobContainerClient(blob.BlobContainerName);
                var snapBlob = await ListBlobs(container, blob.Name, BlobTraits.None, BlobStates.Snapshots)
                    .Where(b => !string.IsNullOrEmpty(b.Snapshot) && b.Name == blob.Name)
                    .AggregateAsync((BlobItem)null, (a, b) => a == null || b == null ? (a ?? b) : (ParseSnapshotDate(b.Snapshot) > ParseSnapshotDate(b.Snapshot) ? a : b));

                metadata.Snapshot = snapBlob?.Snapshot;
            }
        }

        return metadata;
    }

    private BlockBlobClient GetBlockBlob(StoreLocation location, string snapshot = null)
    {
        var container = _blobStorage.GetBlobContainerClient(SafeContainerName(location.Container));

        BlockBlobClient blob;
        if (location.Id.HasValue)
        {
            if (!string.IsNullOrEmpty(location.BasePath))
            {
                var path = Path.Combine(location.BasePath, location.Id.ToString() + IdExtension);
                blob = container.GetBlockBlobClient(SafePath.MakeSafeFilePath(path));
            }
            else
            {
                blob = container.GetBlockBlobClient(location.Id.ToString() + IdExtension);
            }
        }
        else
        {
            blob = container.GetBlockBlobClient(SafePath.MakeSafeFilePath(location.BasePath));
        }

        return string.IsNullOrWhiteSpace(snapshot) ? blob : blob.WithSnapshot(snapshot);
    }

    private string SafeContainerName(string containerName)
    {
        if(_containerPrefix != null)
        {
            containerName = _containerPrefix + containerName;
        }

        if(containerName.Length > 63)
        {
            throw new ArgumentException("Container name cannot be longer than 63 chars", nameof(containerName));
        }

        if(containerName.Length < 3)
        {
            containerName = containerName.PadLeft(3, '0');
        }

        return containerName;
    }

    private static AuditInfo TransformAuditInformation(Metadata current, UpdateAuditInfo newAudit)
    {
        var info = current == null ? new AuditInfo() : current.Audit;
        info.UpdatedBy = newAudit == null ? "0" : newAudit.UpdatedBy;
        info.UpdatedByName = newAudit == null ? string.Empty : newAudit.UpdatedByName;
        info.UpdatedOn = DateTime.UtcNow;

        info.CreatedBy ??= info.UpdatedBy;
        info.CreatedByName ??= info.UpdatedByName;
        info.CreatedOn ??= info.UpdatedOn;

        return info;
    }

    private static IAsyncEnumerable<BlobItem> ListBlobs(BlobContainerClient container, string prefix, BlobTraits traits, BlobStates states)
    {
        // Clean up the prefix if required
        prefix = prefix == null ? null : SafePath.MakeSafeFilePath(prefix);

        return container.GetBlobsAsync(traits, states, prefix);
    }

    private static async IAsyncEnumerable<BlobLease> LeaseRenewInterval(BlobLeaseClient client, BlobRequestConditions condition, [EnumeratorCancellation] CancellationToken token = default)
    {
        await foreach (var _ in AsyncEnumerableExtensions.Interval(LeaseRenewDuration, token))
        {
            yield return await client.RenewAsync(condition, token);
        }
    }

    private static async Task<Tuple<IAsyncDisposable, string, Task>> LockInternal(BlockBlobClient blob)
    {
        BlobLease lease;

        var client = blob.GetBlobLeaseClient();
        try
        {
            lease = await client.AcquireAsync(LeaseDuration);
        }
        catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.LeaseAlreadyPresent)
        {
            return null;
        }
        catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobNotFound)
        {
            lease = null;
        }

        // May not have had a blob pushed...
        if (lease == null)
        {
            try
            {
                using (var stream = new MemoryStream(new byte[1]))
                {
                    try
                    {
                        await blob.UploadAsync(stream, new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } });
                    }
                    catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobAlreadyExists || e.ErrorCode == BlobErrorCode.ConditionNotMet) { }  // Just eat storage exceptions at this point... something was created obviously
                }
                lease = await client.AcquireAsync(LeaseDuration);
            }
            catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.LeaseAlreadyPresent)
            {
                return null;
            }
        }

        var condition = new BlobRequestConditions { LeaseId = lease.LeaseId };

        // Every 30 secs keep the lock renewed
        var tcs = new TaskCompletionSource();
        var keepAlive = LeaseRenewInterval(client, condition)
            .TakeUntilDisposed(onDispose: async () =>
            {
                try
                {
                    await client.ReleaseAsync(condition);
                }
                catch (Exception e)
                {
                    LeoTrace.WriteLine("Release failed: " + e.Message);
                }
                tcs.TrySetResult();
            }, onError: e => tcs.TrySetException(e));


        return Tuple.Create(keepAlive, lease.LeaseId, tcs.Task);
    }

    private static async Task<BlobProperties> GetBlobProperties(BlockBlobClient blob, CancellationToken token = default)
    {
        try
        {
            return await blob.GetPropertiesAsync(cancellationToken: token);
        }
        catch (RequestFailedException e) when (e.Status == 404)
        {
            return null;
        }
    }

    private static DateTime ParseSnapshotDate(string date)
    {
        return DateTime.Parse(date, null, DateTimeStyles.RoundtripKind);
    }
}