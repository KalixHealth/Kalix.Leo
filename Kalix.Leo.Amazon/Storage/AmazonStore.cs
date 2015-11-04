using Amazon.S3;
using Amazon.S3.Model;
using Kalix.Leo.Storage;
using Kalix.Leo.Streams;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
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

        public async Task<Metadata> SaveData(StoreLocation location, Metadata metadata, Func<IWriteAsyncStream, Task<long?>> savingFunc, CancellationToken token)
        {
            var key = GetObjectKey(location);
            string snapshot = null;
            long? length = null;
            using(var stream = new AmazonMultiUploadStream(_client, _bucket, key, metadata))
            {
                length = await savingFunc(stream).ConfigureAwait(false);
                await stream.Complete(token).ConfigureAwait(false);
                snapshot = stream.VersionId;
            }

            // TODO: Save down real length if required...

            return await GetMetadata(location, snapshot).ConfigureAwait(false);
        }

        public Task<Metadata> SaveMetadata(StoreLocation location, Metadata metadata)
        {
            throw new NotImplementedException();
        }

        public async Task<Metadata> GetMetadata(StoreLocation location, string snapshot = null)
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = _bucket,
                Key = GetObjectKey(location),
                VersionId = snapshot
            };

            try
            {
                var resp = await _client.GetObjectMetadataAsync(request).ConfigureAwait(false);
                return ActualMetadata(resp.Metadata, resp.VersionId, resp.LastModified, resp.ContentLength, resp.Headers.ContentType, resp.ETag);
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
            try
            {
                var request = new GetObjectRequest
                {
                    BucketName = _bucket,
                    Key = GetObjectKey(location),
                    VersionId = snapshot
                };

                var resp = await _client.GetObjectAsync(request).ConfigureAwait(false);
                var metadata = ActualMetadata(resp.Metadata, resp.VersionId, resp.LastModified, resp.ContentLength, resp.Headers.ContentType, resp.ETag);
                return new DataWithMetadata(new ReadStreamWrapper(resp.ResponseStream), metadata);
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

        public IAsyncEnumerable<Snapshot> FindSnapshots(StoreLocation location)
        {
            var key = GetObjectKey(location);

            return ListObjects(key)
                .Where(v => v.Key == key && !v.IsDeleteMarker)
                .Select(GetSnapshotFromVersion)
                .Unwrap();
        }

        public IAsyncEnumerable<LocationWithMetadata> FindFiles(string container, string prefix = null)
        {
            var containerPrefix = container + "\\";
            prefix = containerPrefix + (prefix ?? string.Empty).Replace("/", "\\");

            return ListObjects(prefix)
                .Where(v => v.IsLatest)
                .Select(async v =>
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
                    
                    var loc = new StoreLocation(container, path, id);
                    var snapshot = await GetSnapshotFromVersion(v).ConfigureAwait(false);
                    return new LocationWithMetadata(loc, snapshot.Metadata);
                })
                .Unwrap();
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
            var snapshots = ListObjects(key)
                .Where(v => v.Key == key) // Make sure we are not getting anything unexpected
                .Select(v => new KeyVersion { Key = v.Key, VersionId = v.VersionId });

            // Max 1000 objects to delete at once
            await snapshots
                .Buffer(1000)
                .Select(d =>
                {
                    var delRequest = new DeleteObjectsRequest
                    {
                        BucketName = _bucket,
                        Objects = d.ToList(),
                        Quiet = true
                    };

                    return _client.DeleteObjectsAsync(delRequest);
                })
                .Unwrap()
                .LastOrDefault()
                .ConfigureAwait(false);
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
            var snapshots = ListObjects(container + "/")
                .Select(v => new KeyVersion { Key = v.Key, VersionId = v.VersionId });

            await snapshots
                .Buffer(1000)
                .Select(d =>
                {
                    var delRequest = new DeleteObjectsRequest
                    {
                        BucketName = _bucket,
                        Objects = d.ToList(),
                        Quiet = true
                    };

                    return _client.DeleteObjectsAsync(delRequest);
                })
                .Unwrap()
                .LastOrDefault()
                .ConfigureAwait(false); // Make sure we do not throw an exception if no snapshots to delete;
        }

        public static Metadata ActualMetadata(MetadataCollection m, string versionId, DateTime modified, long size, string contentType, string eTag)
        {
            var metadata = new Metadata(m.Keys.ToDictionary(s => s.Replace("x-amz-meta-", string.Empty), s => m[s]));

            metadata.StoredContentType = contentType;
            metadata.StoredContentLength = size;
            metadata.ETag = eTag;
            metadata.Snapshot = versionId;
            metadata.StoredLastModified = modified;
            if (!metadata.LastModified.HasValue)
            {
                metadata.LastModified = modified;
            }

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
            var res = await _client.GetObjectMetadataAsync(req).ConfigureAwait(false);

            return new Snapshot
            {
                Id = version.VersionId,
                Metadata = ActualMetadata(res.Metadata, res.VersionId, res.LastModified, res.ContentLength, res.Headers.ContentType, res.ETag)
            };
        }

        private IAsyncEnumerable<S3ObjectVersion> ListObjects(string prefix)
        {
            return AsyncEnumerableEx.Create<S3ObjectVersion>(async y =>
            {
                bool isComplete = false;
                string keyMarker = null;
                string versionIdMarker = null;

                while (!isComplete && !y.CancellationToken.IsCancellationRequested)
                {
                    var request = new ListVersionsRequest
                    {
                        BucketName = _bucket,
                        Prefix = prefix,
                        KeyMarker = keyMarker,
                        VersionIdMarker = versionIdMarker
                    };

                    var resp = await _client.ListVersionsAsync(request, y.CancellationToken).ConfigureAwait(false);
                    foreach (var v in resp.Versions)
                    {
                        await y.YieldReturn(v).ConfigureAwait(false);
                    }

                    isComplete = !resp.IsTruncated;
                    keyMarker = resp.NextKeyMarker;
                    versionIdMarker = resp.NextVersionIdMarker;
                }
            });
        }
    }
}
