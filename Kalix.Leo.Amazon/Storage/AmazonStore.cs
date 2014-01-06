using Amazon.S3;
using Amazon.S3.Model;
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
        private readonly bool _enableSnapshots;

        public AmazonStore(AmazonS3Client client, bool enableSnapshots)
        {
            _client = client;
            _enableSnapshots = enableSnapshots;
        }

        public Task SaveData(Stream data, StoreLocation location, IDictionary<string, string> metadata = null)
        {
            throw new NotImplementedException();
        }

        public Task<bool> TryOptimisticWrite(Stream data, StoreLocation location, IDictionary<string, string> metadata = null)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> LoadData(StoreLocation location, Func<IDictionary<string, string>, Stream> streamPicker, string snapshot = null)
        {
            var request = new GetObjectRequest
            {
                BucketName = location.Container,
                Key = GetObjectKey(location),
                VersionId = snapshot
            };

            using (var resp = await _client.GetObjectAsync(request))
            {
                if (resp.HttpStatusCode != HttpStatusCode.OK) { return false; }

                var metadata = resp.Metadata.Keys.ToDictionary(s => s, s => resp.Metadata[s]);
                var writeStream = streamPicker(metadata);
                if (writeStream == null) { return false; }

                using (var readStream = resp.ResponseStream)
                {
                    await readStream.CopyToAsync(writeStream);
                }
            }

            return true;
        }

        public async Task<IEnumerable<Snapshot>> FindSnapshots(StoreLocation location)
        {
            var key = GetObjectKey(location);
            var request = new ListVersionsRequest
            {
                BucketName = location.Container,
                Prefix = key
            };

            // TODO: deal with more than 1000 version results
            var resp = await _client.ListVersionsAsync(request);

            return await Task.WhenAll(resp.Versions
                .Where(v => v.Key == key && !v.IsDeleteMarker)
                .OrderByDescending(v => v.LastModified)
                .Select(v => GetSnapshotFromVersion(v, location.Container)));
        }

        public async Task SoftDelete(StoreLocation location)
        {
            // If we support snapshots then we can just delete the record in amazon...
            var key = GetObjectKey(location);
            var request = new DeleteObjectRequest
            {
                BucketName = location.Container,
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
            var request = new ListVersionsRequest
            {
                BucketName = location.Container,
                Prefix = key
            };

            // TODO: deal with more than 1000 version results
            var resp = await _client.ListVersionsAsync(request);
            var toDelete = resp.Versions.Where(v => v.Key == key).Select(v => new KeyVersion
            {
                Key = v.Key,
                VersionId = v.VersionId
            }).ToList();

            if(toDelete.Any())
            {            
                var delRequest = new DeleteObjectsRequest
                {
                    BucketName = location.Container,
                    Objects = toDelete,
                    Quiet = true
                };

                var delRes = await _client.DeleteObjectsAsync(delRequest);
                // TODO: handle failures??
                if (resp.HttpStatusCode != HttpStatusCode.OK)
                {
                    // TODO: throw exception?
                }
            }
        }

        public async Task CreateContainerIfNotExists(string container)
        {
            var request = new PutBucketRequest
            {
                BucketName = container,
                CannedACL = S3CannedACL.Private,
                UseClientRegion = true
            };

            var resp = await _client.PutBucketAsync(request);
            if (resp.HttpStatusCode != HttpStatusCode.OK)
            {
                // TODO: throw exception?
            }
            
            // Always make sure this is enabled if required
            if (_enableSnapshots)
            {
                // Turn on versioning if we have snapshots
                var versioningRequest = new PutBucketVersioningRequest
                {
                    BucketName = container,
                    VersioningConfig = new S3BucketVersioningConfig { Status = VersionStatus.Enabled }
                };
                await _client.PutBucketVersioningAsync(versioningRequest);
            }
        }

        public async Task PermanentDeleteContainer(string container)
        {
            var request = new DeleteBucketRequest
            {
                BucketName = container,
                UseClientRegion = true
            };

            var resp = await _client.DeleteBucketAsync(request);
            if (resp.HttpStatusCode != HttpStatusCode.OK)
            {
                // TODO: throw exception?
            }
        }

        private string GetObjectKey(StoreLocation location)
        {
            if (location.Id.HasValue)
            {
                return Path.Combine(location.BasePath, location.Id.Value.ToString() + IdExtension);
            }
            else
            {
                return location.BasePath;
            }
        }

        private async Task<Snapshot> GetSnapshotFromVersion(S3ObjectVersion version, string bucket)
        {
            var req = new GetObjectMetadataRequest
            {
                BucketName = bucket,
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
    }
}
