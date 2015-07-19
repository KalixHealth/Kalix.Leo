using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Kalix.Leo.Amazon.Storage
{
    public class AmazonMultiUploadStream : Stream
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

        public AmazonMultiUploadStream(AmazonS3Client client, string bucket, string key, Metadata metadata)
        {
            _client = client;
            _bucket = bucket;
            _key = key;
            _metadata = metadata;
        }

        public async Task Abort()
        {
            if(_uploader != null)
            {
                await _uploader.Abort().ConfigureAwait(false);
            }
        }

        public async Task<string> Complete()
        {
            string result;
            if(_uploader != null)
            {
                result = await _uploader.Complete().ConfigureAwait(false);
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
                            request.Metadata.Add(m.Key, m.Value);
                        }
                    }

                    var response = await _client.PutObjectAsync(request).ConfigureAwait(false);
                    result = response.VersionId;
                }
            }
            return result;
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _length += count;
            while(count > 0)
            {
                var toCopy = Math.Min(ReadWriteBufferSize - _currentOffset, count);
                Buffer.BlockCopy(buffer, offset, _buffer, _currentOffset, toCopy);

                offset += toCopy;
                count -= toCopy;
                _currentOffset += toCopy;

                if(_currentOffset == ReadWriteBufferSize)
                {
                    if(_uploader == null)
                    {
                        _uploader = new AmazonMultiUpload(_client, _bucket, _key, _metadata);
                    }

                    _uploader.PushBlockOfData(_buffer, _partNumber);
                    _partNumber++;

                    _buffer = new byte[ReadWriteBufferSize];
                    _currentOffset = 0;
                }
            }
        }

        public override long Length
        {
            get { return _length; }
        }

        public override long Position
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }
    }
}
