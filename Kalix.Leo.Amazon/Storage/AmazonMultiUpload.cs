using Amazon.S3;
using Amazon.S3.Model;
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
        private readonly List<Task<UploadPartResponse>> _blocks;

        public AmazonMultiUpload(AmazonS3Client client, string bucket, string key, Metadata metadata)
        {
            _client = client;
            _bucket = bucket;
            _key = key;
            _blocks = new List<Task<UploadPartResponse>>();

            // Fire up the multipart upload request...
            var req = new InitiateMultipartUploadRequest { BucketName = _bucket, Key = key };
            if (metadata != null)
            {
                foreach (var m in metadata)
                {
                    req.Metadata.Add(m.Key, m.Value);
                }
            }

            _uploadId = _client.InitiateMultipartUploadAsync(req).ContinueWith(r => r.Result.UploadId);
        }

        public void PushBlockOfData(byte[] data, int partNumber)
        {
            var task = PushBlockOfDataInteral(data, partNumber);
            _blocks.Add(task);
        }

        public async Task Complete()
        {
            var responses = await Task.WhenAll(_blocks).ConfigureAwait(false);

            var req = new CompleteMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = _key,
                UploadId = await _uploadId.ConfigureAwait(false),
            };

            req.AddPartETags(responses);

            await _client.CompleteMultipartUploadAsync(req).ConfigureAwait(false);
        }

        public async Task Abort()
        {
            // No need to cancel if the uploadid call is the thing that failed
            if (_uploadId.IsFaulted || !_uploadId.IsCompleted) { return; }

            var abortMPURequest = new AbortMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = _key,
                UploadId = _uploadId.Result
            };

            await _client.AbortMultipartUploadAsync(abortMPURequest).ConfigureAwait(false);
        }

        private async Task<UploadPartResponse> PushBlockOfDataInteral(byte[] data, int partNumber)
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

                return await _client.UploadPartAsync(uploadReq).ConfigureAwait(false);
            }
        }
    }
}
