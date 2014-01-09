//using Kalix.Leo.Storage;
//using Microsoft.WindowsAzure.Storage;
//using Microsoft.WindowsAzure.Storage.Blob;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//using System.Net;
//using System.Reactive;
//using System.Reactive.Concurrency;
//using System.Reactive.Linq;
//using System.Threading;
//using System.Threading.Tasks;

//namespace Kalix.Leo.Azure.Storage
//{
//    public class AzureStore : IOptimisticStore
//    {
//        private const string IdExtension = ".id";
//        private const string DefaultDeletedKey = "leo.azurestorage.deleted";
//        private const int AzureBlockSize = 4194304;

//        private readonly CloudBlobClient _blobStorage;
//        private readonly string _deletedKey;
//        private readonly bool _enableSnapshots;

//        /// <summary>
//        /// Constructor for a store backed by Azure
//        /// </summary>
//        /// <param name="blobStorage">The storage account that backs this store</param>
//        /// <param name="enableSnapshots">Whether any save on this store should create snapshots</param>
//        /// <param name="deletedKey">The metadata key to check if a store item is soft deleted</param>
//        public AzureStore(CloudBlobClient blobStorage, bool enableSnapshots, string deletedKey = null)
//        {
//            _blobStorage = blobStorage;
//            _enableSnapshots = enableSnapshots;
//            _deletedKey = deletedKey ?? DefaultDeletedKey;
//        }

//        public Task SaveData(StoreLocation location, DataWithMetadata data, long? length = null, bool? multipart = null)
//        {
//            var blob = GetBlockBlob(location);

//            // Copy the metadata across
//            foreach (var m in data.Metadata)
//            {
//                blob.Metadata[m.Key] = m.Value;
//            }

//            bool useMulti;
//            if (multipart.HasValue)
//            {
//                useMulti = multipart.Value;
//            }
//            else
//            {
//                // Only use multi by default if the size is greater than 10 blocks...
//                useMulti = !length.HasValue || length.Value > 10 * AzureBlockSize;
//            }

//            return useMulti ? SaveDataMultipart(data.Stream, blob) : SaveDataSingle(data.Stream, blob, false);
//        }

//        public Task<bool> TryOptimisticWrite(StoreLocation location, DataWithMetadata data)
//        {
//            var blob = GetBlockBlob(location);

//            // Copy the metadata across
//            foreach (var m in data.Metadata)
//            {
//                blob.Metadata[m.Key] = m.Value;
//            }

//            return SaveDataSingle(data.Stream, blob, true);
//        }

//        private async Task<bool> SaveDataSingle(IObservable<byte[]> data, CloudBlockBlob blob, bool isOptimistic)
//        {
//            try
//            {
//                var condition = isOptimistic ? AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag) : null;
//                await blob.UploadFromStreamAsync(data.ToReadStream(), condition, null, null);

//                // Create a snapshot straight away on azure
//                // Note: this shouldnt matter for cost as any blocks that are the same do not cost extra
//                if (_enableSnapshots)
//                {
//                    await blob.CreateSnapshotAsync();
//                }
//            }
//            catch (StorageException exc)
//            {
//                if (exc.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
//                {
//                    return false;
//                }
//                throw;
//            }

//            return true;
//        }

//        private async Task SaveDataMultipart(IObservable<byte[]> data, CloudBlockBlob blob, int blocksInParallel = 4)
//        {
//            int block = 0;
//            var currentBlock = new byte[AzureBlockSize];
//            int position = 0;

//            await data
//                // Group into blocks of the right size to send up to azure
//                .Select(blockBytes =>
//                {
//                    int count = blockBytes.Length;
//                    int currentPosition = 0;
//                    var streamList = new List<Stream>();

//                    while (count > 0)
//                    {
//                        var dataToWrite = AzureBlockSize - position;
//                        if (dataToWrite > count) { dataToWrite = count; }

//                        Buffer.BlockCopy(blockBytes, currentPosition, currentBlock, position, dataToWrite);
//                        currentPosition += dataToWrite;
//                        position += dataToWrite;
//                        count -= dataToWrite;

//                        if (position == AzureBlockSize)
//                        {
//                            streamList.Add(new MemoryStream(currentBlock));
//                            currentBlock = new byte[AzureBlockSize];
//                            position = 0;
//                        }
//                    }

//                    return streamList.ToObservable(Scheduler.Immediate);
//                })
//                .Merge()
//                .Select(ms =>
//                {
//                    block++;
//                    var blockStr = Convert.ToBase64String(BitConverter.GetBytes(block));
//                    return blob
//                        .PutBlockAsync(blockStr, ms, null)
//                        .ContinueWith(t => blockStr);
//                })

//                // allow n to run simultaneously
//                .Buffer(blocksInParallel)
//                .Select(t => Task.WhenAll(t))

//                // merge it all back together again
//                .Merge()
//                .Select(s => s.ToObservable())
//                .Merge()

//                // finish the put
//                .ToList()
//                .Select(blocks =>
//                {
//                    blob.PutBlockList(blocks);
//                    return Unit.Default;
//                });

//            // Create a snapshot straight away on azure
//            // Note: this shouldnt matter for cost as any blocks that are the same do not cost extra
//            if (_enableSnapshots)
//            {
//                await blob.CreateSnapshotAsync();
//            }
//        }

//        public async Task<IMetadata> GetMetadata(StoreLocation location, string snapshot = null)
//        {
//            var blob = GetBlockBlob(location, snapshot);
//            try
//            {
//                await blob.FetchAttributesAsync();
//            }
//            catch (StorageException e)
//            {
//                if (e.RequestInformation.HttpStatusCode == 404)
//                {
//                    return null;
//                }

//                throw;
//            }

//            return GetActualMetadata(blob);
//        }

//        public Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null)
//        {
//            var blob = GetBlockBlob(location, snapshot);

//            var cancellation = new CancellationTokenSource();
//            var source = new TaskCompletionSource<DataWithMetadata>();

//            IObservable<byte[]> data = null;
//            data = Observable.Create<byte[]>((obs, ct) =>
//            {
//                return obs.UseWriteStream(
//                    async s =>
//                    {
//                        try
//                        {
//                            await blob.DownloadToStreamAsync(s, ct);
//                        }
//                        catch (StorageException e)
//                        {
//                            if (e.RequestInformation.HttpStatusCode == 404)
//                            {
//                                source.TrySetResult(null);
//                            }
//                            else
//                            {
//                                throw;
//                            }
//                        }
//                    },
//                    () =>
//                    {
//                        var metadata = GetActualMetadata(blob);
//                        if (metadata.ContainsKey(_deletedKey))
//                        {
//                            source.TrySetResult(null);
//                            cancellation.Cancel();
//                        }
//                        else
//                        {
//                            source.TrySetResult(new DataWithMetadata(data, metadata));
//                        }
//                    }
//                );
//            });

//            data.Subscribe(cancellation.Token);
//            return source.Task;
//        }

//        public IObservable<Snapshot> FindSnapshots(StoreLocation location)
//        {
//            var blob = GetBlockBlob(location);
//            var results = Observable.Create<ICloudBlob>((observer, ct) =>
//            {
//                return ListBlobs(observer, blob.Container, blob.Name, BlobListingDetails.Snapshots | BlobListingDetails.Metadata, ct);
//            });

//            return results
//                .Where(b => b.Uri == blob.Uri && b.SnapshotTime.HasValue)
//                .Select(b => new Snapshot
//                {
//                    Id = b.SnapshotTime.Value.UtcDateTime.Ticks.ToString(),
//                    Modified = b.SnapshotTime.Value.UtcDateTime,
//                    Metadata = GetActualMetadata(b)
//                });
//        }

//        public IObservable<StoreLocation> FindFiles(string container, string prefix = null)
//        {
//            var results = Observable.Create<ICloudBlob>((observer, ct) =>
//            {
//                var c = _blobStorage.GetContainerReference(container);
//                return ListBlobs(observer, c, prefix, BlobListingDetails.Metadata, ct);
//            });

//            return results
//                .Where(b => !b.Metadata.ContainsKey(_deletedKey)) // Do not include blobs which are soft deleted
//                .Select(b =>
//                {
//                    long? id = null;
//                    string path = b.Name;
//                    if (path.EndsWith(IdExtension))
//                    {
//                        long tempId;
//                        if (long.TryParse(Path.GetFileNameWithoutExtension(path), out tempId))
//                        {
//                            id = tempId;
//                            path = Path.GetDirectoryName(path);
//                        }
//                    }
//                    return new StoreLocation(container, path, id);
//                });
//        }

//        private async Task ListBlobs(IObserver<ICloudBlob> observer, CloudBlobContainer container, string prefix, BlobListingDetails options, CancellationToken ct)
//        {
//            BlobContinuationToken token = new BlobContinuationToken();

//            try
//            {
//                do
//                {
//                    var segment = await container.ListBlobsSegmentedAsync(prefix, true, options, null, token, null, null, ct);

//                    foreach (var blob in segment.Results.OfType<ICloudBlob>())
//                    {
//                        observer.OnNext(blob);
//                    }

//                    token = segment.ContinuationToken;
//                }
//                while (token != null && !ct.IsCancellationRequested);

//                ct.ThrowIfCancellationRequested(); // Just in case...
//                observer.OnCompleted();
//            }
//            catch (Exception e)
//            {
//                observer.OnError(e);
//            }
//        }

//        public async Task SoftDelete(StoreLocation location)
//        {
//            // In Azure we cannot delete the blob as this will loose the snapshots
//            // Instead we will just add some metadata
//            var blob = GetBlockBlob(location);
//            try
//            {
//                await blob.FetchAttributesAsync();
//            }
//            catch (StorageException e)
//            {
//                if (e.RequestInformation.HttpStatusCode == 404)
//                {
//                    return;
//                }
//                throw;
//            }

//            blob.Metadata[_deletedKey] = DateTime.UtcNow.Ticks.ToString();
//            await blob.SetMetadataAsync();
//        }

//        public Task PermanentDelete(StoreLocation location)
//        {
//            var blob = GetBlockBlob(location);
//            return blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, null, null, null);
//        }

//        public Task CreateContainerIfNotExists(string container)
//        {
//            var c = _blobStorage.GetContainerReference(container);
//            return c.CreateIfNotExistsAsync();
//        }

//        public Task PermanentDeleteContainer(string container)
//        {
//            var c = _blobStorage.GetContainerReference(container);
//            return c.DeleteIfExistsAsync();
//        }

//        private IMetadata GetActualMetadata(ICloudBlob blob)
//        {
//            var metadata = new Metadata(blob.Metadata);

//            if (!metadata.ContainsKey(MetadataConstants.ModifiedMetadataKey) && blob.Properties.LastModified.HasValue)
//            {
//                metadata[MetadataConstants.ModifiedMetadataKey] = blob.Properties.LastModified.Value.UtcDateTime.Ticks.ToString();
//            }

//            if (!metadata.ContainsKey(MetadataConstants.SizeMetadataKey))
//            {
//                metadata[MetadataConstants.SizeMetadataKey] = blob.Properties.Length.ToString();
//            }

//            return metadata;
//        }

//        private CloudBlockBlob GetBlockBlob(StoreLocation location, string snapshot = null)
//        {
//            DateTime? snapshotDate = null;
//            if (snapshot != null)
//            {
//                snapshotDate = new DateTime(long.Parse(snapshot));
//            }

//            var container = _blobStorage.GetContainerReference(location.Container);
//            var offset = snapshotDate.HasValue ? new DateTimeOffset(snapshotDate.Value, new TimeSpan(0)) : (DateTimeOffset?)null;

//            CloudBlockBlob blob;
//            if (location.Id.HasValue)
//            {
//                if (!string.IsNullOrEmpty(location.BasePath))
//                {
//                    var dir = container.GetDirectoryReference(location.BasePath);
//                    blob = dir.GetBlockBlobReference(location.Id.ToString() + IdExtension, offset);
//                }
//                else
//                {
//                    blob = container.GetBlockBlobReference(location.Id.ToString() + IdExtension, offset);
//                }
//            }
//            else
//            {
//                blob = container.GetBlockBlobReference(location.BasePath, offset);
//            }

//            return blob;
//        }
//    }
//}
