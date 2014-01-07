using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
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
                    var metadata = resp.Metadata.Keys.ToDictionary(s => s, s => resp.Metadata[s]);
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
                if(e.ErrorCode == "NotFound")
                {
                    return false;
                }
            }

            return true;
        }

        public async Task<IEnumerable<Snapshot>> FindSnapshots(StoreLocation location)
        {
            var key = GetObjectKey(location);
            var snapshots = await ListObjects(key);

            return await Task.WhenAll(snapshots
                .Where(v => v.Key == key && !v.IsDeleteMarker)
                .OrderByDescending(v => v.LastModified)
                .Select(GetSnapshotFromVersion));
        }

        public async Task SoftDelete(StoreLocation location)
        {
            // If we support snapshots then we can just delete the record in amazon...
            var key = GetObjectKey(location);
            var request = new DeleteObjectRequest
            {
                BucketName = _bucket,
                Key = key
            };

            var resp = await _client.DeleteObjectAsync(request);
            if (resp.HttpStatusCode != HttpStatusCode.OK)
            {
                // TODO: throw exception?
            }
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

                    var delRes = await _client.DeleteObjectsAsync(delRequest);
                    if (delRes.HttpStatusCode != HttpStatusCode.OK)
                    {
                        // TODO: throw exception?
                    }
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
            var snapshots = await ListObjects(container + "/");

            if (snapshots.Any())
            {
                var delTasks = Chunk(snapshots.Select(v => new KeyVersion { Key = v.Key, VersionId = v.VersionId }), 1000).Select(async d =>
                {
                    var delRequest = new DeleteObjectsRequest
                    {
                        BucketName = _bucket,
                        Objects = d,
                        Quiet = true
                    };

                    var delRes = await _client.DeleteObjectsAsync(delRequest);
                    if (delRes.HttpStatusCode != HttpStatusCode.OK)
                    {
                        // TODO: throw exception?
                    }
                });

                await Task.WhenAll(delTasks);
            }
        }

        private string GetObjectKey(StoreLocation location)
        {
            string objPath;
            if (location.Id.HasValue)
            {
                objPath = Path.Combine(location.BasePath, location.Id.Value.ToString() + IdExtension);
            }
            else
            {
                objPath = location.BasePath;
            }

            return Path.Combine(location.Container, objPath);
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
                Metadata = res.Metadata.Keys.ToDictionary(s => s, s => res.Metadata[s])
            };
        }

        private async Task<IEnumerable<S3ObjectVersion>> ListObjects(string prefix)
        {
            var results = new List<S3ObjectVersion>();
            bool isComplete = false;
            string keyMarker = null;
            string versionIdMarker = null;

            while (!isComplete)
            {
                var request = new ListVersionsRequest
                {
                    BucketName = _bucket,
                    Prefix = prefix,
                    KeyMarker = keyMarker,
                    VersionIdMarker = versionIdMarker
                };

                var resp = await _client.ListVersionsAsync(request);
                results.AddRange(resp.Versions);

                isComplete = !resp.IsTruncated;
                keyMarker = resp.NextKeyMarker;
                versionIdMarker = resp.NextVersionIdMarker;
            }

            return results;
        }

        private IEnumerable<List<T>> Chunk<T>(IEnumerable<T> en, int size)
        {
            var enumerator = en.GetEnumerator();
            var currentList = new List<T>(size);

            while (enumerator.MoveNext())
            {
                currentList.Add(enumerator.Current);
                if (currentList.Count == size)
                {
                    yield return currentList;
                    currentList = new List<T>(size);
                }
            }

            if (currentList.Any())
            {
                yield return currentList;
            }
        }
    }
}
