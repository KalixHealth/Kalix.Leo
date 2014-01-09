using Amazon.S3;
using Amazon.S3.Model;
using Kalix.Leo.Storage;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo.Amazon.Storage
{
    public class AmazonMultiUpload
    {
        private readonly AmazonS3Client _client;
        private readonly string _bucket;
        private readonly string _key;
        private readonly Task<string> _uploadId;
        private readonly List<Task> _blocks;

        public AmazonMultiUpload(AmazonS3Client client, string bucket, string key, IMetadata metadata)
        {
            _client = client;
            _bucket = bucket;
            _key = key;
            _blocks = new List<Task>();

            // Fire up the multipart upload request...
            var req = new InitiateMultipartUploadRequest { BucketName = _bucket, Key = key };
            foreach (var m in metadata)
            {
                req.Metadata.Add(m.Key, m.Value);
            }

            _uploadId = _client.InitiateMultipartUploadAsync(req).ContinueWith(r => r.Result.UploadId);
        }

        public Task PushBlockOfData(byte[] data, int partNumber)
        {
            var task = PushBlockOfDataInteral(data, partNumber);
            _blocks.Add(task);
            return task;
        }

        public async Task Complete()
        {
            await Task.WhenAll(_blocks).ConfigureAwait(false);

            var req = new CompleteMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = _key,
                UploadId = await _uploadId.ConfigureAwait(false),
            };

            await _client.CompleteMultipartUploadAsync(req).ConfigureAwait(false);
        }

        private async Task PushBlockOfDataInteral(byte[] data, int partNumber)
        {
            var uploadId = await _uploadId.ConfigureAwait(false);

            using (var ms = new MemoryStream(data))
            {
                var uploadReq = new UploadPartRequest
                {
                    BucketName = _bucket,
                    Key = _key,
                    UploadId = uploadId,
                    InputStream = ms,
                    PartNumber = partNumber,
                    PartSize = data.LongLength
                };

                await _client.UploadPartAsync(uploadReq).ConfigureAwait(false);
            }
        }
    }
}
