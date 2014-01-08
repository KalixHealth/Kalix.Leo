using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Amazon.Storage
{
    public class AmazonStore : IStore
    {
        private const string IdExtension = ".dat";

        private readonly AmazonS3Client _client;
        private readonly string _bucket;

        /// <summary>
        /// The store is a wrapper over a specific bucket. If you want snapshots to work make sure they
        /// are enabled on the bucket!
        /// </summary>
        /// <param name="client">Amazon client that can be configured</param>
        /// <param name="bucket">Bucket to place everything</param>
        public AmazonStore(AmazonS3Client client, string bucket)
        {
            _client = client;
            _bucket = bucket;
        }

        public Task SaveData(Stream data, StoreLocation location, IDictionary<string, string> metadata = null, bool multipart = false)
        {
            return multipart ? SaveDataMultipart(data, location, metadata) : SaveDataSingle(data, location, metadata);
        }

        private Task SaveDataSingle(Stream data, StoreLocation location, IDictionary<string, string> metadata)
        {
            var request = new PutObjectRequest
            {
                AutoCloseStream = false,
                AutoResetStreamPosition = false,
                BucketName = _bucket,
                Key = GetObjectKey(location),
                InputStream = data
            };

            // Copy the metadata across
            if (metadata != null)
            {
                foreach (var m in metadata)
                {
                    request.Metadata.Add(m.Key, m.Value);
                }
            }

            return _client.PutObjectAsync(request);
        }

        private Task SaveDataMultipart(Stream data, StoreLocation location, IDictionary<string, string> metadata)
        {
            var request = new TransferUtilityUploadRequest
            {
                AutoCloseStream = false,
                AutoResetStreamPosition = false,
                BucketName = _bucket,
                Key = GetObjectKey(location),
                PartSize = 4194304, // 4mb
                InputStream = data
            };

            // Copy the metadata across
            if (metadata != null)
            {
                foreach (var m in metadata)
                {
                    request.Metadata.Add(m.Key, m.Value);
                }
            }

            var util = new TransferUtility(_client);
            return util.UploadAsync(request);
        }

        public async Task<IDictionary<string, string>> GetMetadata(StoreLocation location, string snapshot = null)
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = _bucket,
                Key = GetObjectKey(location),
                VersionId = snapshot
            };

            try
            {
                var resp = await _client.GetObjectMetadataAsync(request);
                return ActualMetadata(resp.Metadata, resp.LastModified, resp.ContentLength);
            }
            catch (AmazonS3Exception e)
            {
                if (e.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }

                throw;
            }
        }

        public async Task<bool> LoadData(StoreLocation location, Func<IDictionary<string, string>, Stream> streamPicker, string snapshot = null)
        {
            var request = new GetObjectRequest
            {
                BucketName = _bucket,
                Key = GetObjectKey(location),
                VersionId = snapshot
            };

            try
            {
                using (var resp = await _client.GetObjectAsync(request))
                {
                    var metadata = ActualMetadata(resp.Metadata, resp.LastModified, resp.ContentLength);
                    var writeStream = streamPicker(metadata);
                    if (writeStream == null) { return false; }

                    using (var readStream = resp.ResponseStream)
                    {
                        await readStream.CopyToAsync(writeStream);
                    }
                }
            }
            catch(AmazonS3Exception e)
            {
                if(e.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }

                throw;
            }

            return true;
        }

        public IObservable<Snapshot> FindSnapshots(StoreLocation location)
        {
            var key = GetObjectKey(location);
            var snapshots = await ListObjects(key);

            return await Task.WhenAll(snapshots
                .Where(v => v.Key == key && !v.IsDeleteMarker)
                .OrderByDescending(v => v.LastModified)
                .Select(GetSnapshotFromVersion));
        }

        public IObservable<StoreLocation> FindFiles(string container, string prefix = null)
        {
        }

        public Task SoftDelete(StoreLocation location)
        {
            // If we support snapshots then we can just delete the record in amazon...
            var key = GetObjectKey(location);
            var request = new DeleteObjectRequest
            {
                BucketName = _bucket,
                Key = key
            };

            return _client.DeleteObjectAsync(request);
        }

        public async Task PermanentDelete(StoreLocation location)
        {
            var key = GetObjectKey(location);
            var snapshots = await ListObjects(key);

            var toDelete = snapshots.Where(v => v.Key == key).Select(v => new KeyVersion
            {
                Key = v.Key,
                VersionId = v.VersionId
            }).ToList();

            if(toDelete.Any())
            {
                var delTasks = Chunk(toDelete, 1000).Select(async d =>
                {
                    var delRequest = new DeleteObjectsRequest
                    {
                        BucketName = _bucket,
                        Objects = d,
                        Quiet = true
                    };

                    await _client.DeleteObjectsAsync(delRequest);
                });

                await Task.WhenAll(delTasks);
            }
        }

        public Task CreateContainerIfNotExists(string container)
        {
            // Don't need to do anything here...
            // As the bucket exists and the container is virtual
            return Task.FromResult(0);
        }

        public async Task PermanentDeleteContainer(string container)
        {
            // We have to iterate though every object in a container and then delete it...
            var snapshots = Observable
                .Create<S3ObjectVersion>((observer, ct) => ListObjects(observer, container + "/", ct))
                .Select(v => new KeyVersion { Key = v.Key, VersionId = v.VersionId });

            var delTasks = snapshots.Buffer(1000).Select(d =>
            {
                var delRequest = new DeleteObjectsRequest
                {
                    BucketName = _bucket,
                    Objects = d.ToList(),
                    Quiet = true
                };

                return _client.DeleteObjectsAsync(delRequest);
            });

            // Make sure we are not blocking anything here!
            var tasks = new List<Task>();
            await delTasks.ForEachAsync(t => tasks.Add(t));
            await Task.WhenAll(tasks);
        }

        private IDictionary<string, string> ActualMetadata(MetadataCollection m, DateTime modified, long size)
        {
            var metadata = m.Keys.ToDictionary(s => s.Replace("x-amz-meta-", string.Empty), s => m[s]);
            if(!metadata.ContainsKey(MetadataConstants.SizeMetadataKey))
            {
                metadata[MetadataConstants.SizeMetadataKey] = size.ToString();
            }

            if(!metadata.ContainsKey(MetadataConstants.ModifiedMetadataKey))
            {
                metadata[MetadataConstants.ModifiedMetadataKey] = modified.Ticks.ToString();
            }
            return metadata;
        }

        private string GetObjectKey(StoreLocation location)
        {
            var list = new List<string>(3);
            list.Add(location.Container);

            if(!string.IsNullOrEmpty(location.BasePath))
            {
                list.Add(location.BasePath);
            }

            if (location.Id.HasValue)
            {
                list.Add(location.Id.Value.ToString() + IdExtension);
            }

            return string.Join("\\", list);
        }

        private async Task<Snapshot> GetSnapshotFromVersion(S3ObjectVersion version)
        {
            var req = new GetObjectMetadataRequest
            {
                BucketName = _bucket,
                Key = version.Key,
                VersionId = version.VersionId
            };
            var res = await _client.GetObjectMetadataAsync(req);

            return new Snapshot
            {
                Id = version.VersionId,
                Modified = version.LastModified,
                Metadata = ActualMetadata(res.Metadata, res.LastModified, res.ContentLength)
            };
        }

        private async Task ListObjects(IObserver<S3ObjectVersion> observer, string prefix, CancellationToken ct)
        {
            try
            {
                bool isComplete = false;
                string keyMarker = null;
                string versionIdMarker = null;

                while (!isComplete && !ct.IsCancellationRequested)
                {
                    var request = new ListVersionsRequest
                    {
                        BucketName = _bucket,
                        Prefix = prefix,
                        KeyMarker = keyMarker,
                        VersionIdMarker = versionIdMarker
                    };

                    var resp = await _client.ListVersionsAsync(request, ct);
                    foreach (var v in resp.Versions)
                    {
                        observer.OnNext(v);
                    }

                    isComplete = !resp.IsTruncated;
                    keyMarker = resp.NextKeyMarker;
                    versionIdMarker = resp.NextVersionIdMarker;
                }

                ct.ThrowIfCancellationRequested(); // Just in case...
                observer.OnCompleted();
            }
            catch (Exception e)
            {
                observer.OnError(e);
            }
        }
    }
}
