using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Amazon.Storage
{
    public class AmazonMultiUploadStream : IWriteAsyncStream
    {
        private const int ReadWriteBufferSize = 6291456; // 6Mb

        private readonly AmazonS3Client _client;
        private readonly string _bucket;
        private readonly string _key;
        private readonly Metadata _metadata;

        private int _partNumber = 1;
        private int _currentOffset = 0;
        private byte[] _buffer = new byte[ReadWriteBufferSize];
        private long _length = 0;

        private AmazonMultiUpload _uploader;
        private bool _isComplete;

        public AmazonMultiUploadStream(AmazonS3Client client, string bucket, string key, Metadata metadata)
        {
            _client = client;
            _bucket = bucket;
            _key = key;
            _metadata = metadata;
        }

        public string VersionId { get; private set; }

        public Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            if (_isComplete) { throw new InvalidOperationException("Cannot write anymore to a completed stream"); }

            _length += count;
            while (count > 0)
            {
                var toCopy = Math.Min(ReadWriteBufferSize - _currentOffset, count);
                Buffer.BlockCopy(buffer, offset, _buffer, _currentOffset, toCopy);

                offset += toCopy;
                count -= toCopy;
                _currentOffset += toCopy;

                if (_currentOffset == ReadWriteBufferSize)
                {
                    if (_uploader == null)
                    {
                        _uploader = new AmazonMultiUpload(_client, _bucket, _key, _metadata, ct);
                    }

                    _uploader.PushBlockOfData(_buffer, _partNumber);
                    _partNumber++;

                    _buffer = new byte[ReadWriteBufferSize];
                    _currentOffset = 0;
                }
            }
            return Task.FromResult(0);
        }

        public async Task Complete(CancellationToken ct)
        {
            if (!_isComplete)
            {
                string versionId;
                if (_uploader != null)
                {
                    versionId = await _uploader.Complete().ConfigureAwait(false);
                }
                else
                {
                    // Just a single upload request...
                    using (var ms = new MemoryStream(_buffer, 0, _currentOffset))
                    {
                        var request = new PutObjectRequest
                        {
                            AutoCloseStream = false,
                            AutoResetStreamPosition = false,
                            BucketName = _bucket,
                            Key = _key,
                            InputStream = ms
                        };

                        // Copy the metadata across
                        if (_metadata != null)
                        {
                            foreach (var m in _metadata)
                            {
                                request.Metadata.Add("x-amz-meta-" + m.Key, m.Value);
                            }
                        }

                        var resp = await _client.PutObjectAsync(request, ct).ConfigureAwait(false);
                        versionId = resp.VersionId;
                    }
                }
                VersionId = versionId;
                _isComplete = true;
                _uploader = null;
            }
        }

        public Task Cancel()
        {
            Dispose();
            return Task.FromResult(0);
        }

        public Task FlushAsync(CancellationToken ct)
        {
            return Task.FromResult(0);
        }

        public void Dispose()
        {
            if (_uploader != null)
            {
                _uploader.Abort();
                _uploader = null;
            }
            _isComplete = true;
        }
    }
}
