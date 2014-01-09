using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Amazon.Storage
{
    public class AmazonStore : IStore
    {
        private const string IdExtension = ".dat";
        private const int ReadWriteBufferSize = 6291456; // 6Mb

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

        public async Task SaveData(StoreLocation location, DataWithMetadata data)
        {
            var key = GetObjectKey(location);
            byte[] firstHit = null;
            var uploadLock = new object();
            AmazonMultiUpload uploadClient = null;
            int partNumber = 1;

            await data.Stream
                .BufferBytes(ReadWriteBufferSize)
                .Select(async b =>
                {
                    if (firstHit == null)
                    {
                        firstHit = b;
                    }
                    else
                    {
                        var pNum = Interlocked.Increment(ref partNumber);

                        if (uploadClient == null)
                        {
                            lock (uploadLock)
                            {
                                if (uploadClient == null)
                                {
                                    uploadClient = new AmazonMultiUpload(_client, _bucket, key, data.Metadata);
                                }
                            }
                        }

                        if (pNum == 2)
                        {
                            await uploadClient.PushBlockOfData(firstHit, 1);
                        }

                        // We are doing multipart here!
                        await uploadClient.PushBlockOfData(b, pNum);
                    }

                    return Unit.Default;
                })
                .Merge();

            // If we didnt do multimerge then just do single!
            if (uploadClient == null)
            {
            }
            else
            {
                await uploadClient.Complete();
            }
        }

        private async Task SaveDataSingle(StoreLocation location, byte[] data, IMetadata metadata)
        {
            using (var ms = new MemoryStream(data))
            {
                var request = new PutObjectRequest
                {
                    AutoCloseStream = false,
                    AutoResetStreamPosition = false,
                    BucketName = _bucket,
                    Key = GetObjectKey(location),
                    InputStream = ms
                };

                // Copy the metadata across
                foreach (var m in metadata)
                {
                    request.Metadata.Add(m.Key, m.Value);
                }

                await _client.PutObjectAsync(request).ConfigureAwait(false);
            }
        }

        public async Task<IMetadata> GetMetadata(StoreLocation location, string snapshot = null)
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

        public async Task<DataWithMetadata> LoadData(StoreLocation location, string snapshot = null)
        {
            var request = new GetObjectRequest
            {
                BucketName = _bucket,
                Key = GetObjectKey(location),
                VersionId = snapshot
            };

            try
            {
                var resp = await _client.GetObjectAsync(request);
                var metadata = ActualMetadata(resp.Metadata, resp.LastModified, resp.ContentLength);
                var stream = resp.ResponseStream.ToObservable(ReadWriteBufferSize, () =>
                {
                    resp.ResponseStream.Dispose();
                    resp.Dispose();
                });

                return new DataWithMetadata(stream, metadata);
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

        public IObservable<Snapshot> FindSnapshots(StoreLocation location)
        {
            var key = GetObjectKey(location);
            return Observable
                .Create<S3ObjectVersion>((observer, ct) => ListObjects(observer, key, ct))
                .Where(v => v.Key == key && !v.IsDeleteMarker) // Make sure we are not getting anything unexpected
                .Select(GetSnapshotFromVersion)
                .Merge();
        }

        public IObservable<StoreLocation> FindFiles(string container, string prefix = null)
        {
            var containerPrefix = container + "\\";
            return Observable
                .Create<S3ObjectVersion>((observer, ct) => ListObjects(observer, containerPrefix + (prefix ?? string.Empty).Replace("/", "\\"), ct))
                .Where(v => v.IsLatest)
                .Select(v =>
                {
                    var path = v.Key.Remove(0, containerPrefix.Length);
                    long? id = null;
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

            // We have to iterate though every object and delete it...
            var snapshots = Observable
                .Create<S3ObjectVersion>((observer, ct) => ListObjects(observer, key, ct))
                .Where(v => v.Key == key) // Make sure we are not getting anything unexpected
                .Select(v => new KeyVersion { Key = v.Key, VersionId = v.VersionId });

            await snapshots.Buffer(1000).Select(d =>
            {
                var delRequest = new DeleteObjectsRequest
                {
                    BucketName = _bucket,
                    Objects = d.ToList(),
                    Quiet = true
                };

                return _client.DeleteObjectsAsync(delRequest);
            })
            .Merge()
            .LastOrDefaultAsync(); // Make sure we do not throw an exception if no snapshots to delete
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

            await snapshots.Buffer(1000).Select(d =>
            {
                var delRequest = new DeleteObjectsRequest
                {
                    BucketName = _bucket,
                    Objects = d.ToList(),
                    Quiet = true
                };

                return _client.DeleteObjectsAsync(delRequest);
            })
            .Merge()
            .LastOrDefaultAsync(); // Make sure we do not throw an exception if no snapshots to delete;
        }

        private IMetadata ActualMetadata(MetadataCollection m, DateTime modified, long size)
        {
            var metadata = new Metadata(m.Keys.ToDictionary(s => s.Replace("x-amz-meta-", string.Empty), s => m[s]));
            metadata.Size = size;
            metadata.LastModified = modified;
            return metadata;
        }

        private string GetObjectKey(StoreLocation location)
        {
            var list = new List<string>(3);
            list.Add(location.Container);

            if (!string.IsNullOrEmpty(location.BasePath))
            {
                list.Add(location.BasePath.Replace("/", "\\"));
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
