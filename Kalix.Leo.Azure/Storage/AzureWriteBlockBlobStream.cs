using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage
{
    public class AzureWriteBlockBlobStream : Stream
    {
        private const int AzureBlockSize = 4194304;

        private readonly CloudBlockBlob _blob;
        private readonly AccessCondition _condition;
        private readonly List<Task> _uploads;
        
        private byte[] _buffer = null;
        private int _offset = 0;
        private int _partNumber = 1;
        private long _length = 0;

        private bool _hasCompleted;

        public AzureWriteBlockBlobStream(CloudBlockBlob blob, AccessCondition condition)
        {
            _blob = blob;
            _condition = condition;
            _uploads = new List<Task>();
        }

        // Both sync and async can use this version...
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (_hasCompleted)
            {
                throw new InvalidOperationException("The azure write stream has already been completed");
            }

            if (_buffer == null)
            {
                _buffer = new byte[AzureBlockSize];
            }

            _length += count;
            while (count > 0)
            {
                var read = Math.Min(_buffer.Length - _offset, count);
                Buffer.BlockCopy(buffer, offset, _buffer, _offset, read);

                _offset += read;
                count -= read;
                offset += read;

                if (_offset == _buffer.Length)
                {
                    var key = GetKey(_partNumber);
                    _uploads.Add(PutBlob(key, _buffer, _buffer.Length));

                    _buffer = new byte[AzureBlockSize];
                    _offset = 0;
                    _partNumber++;
                }
            }
        }

        public async Task Complete()
        {
            if (!_hasCompleted)
            {
                _hasCompleted = true;

                if (_partNumber == 1)
                {
                    // We haven't even uploaded one block yet... just upload it straight...
                    using (var ms = new MemoryStream(_buffer, 0, _offset))
                    {
                        await _blob.UploadFromStreamAsync(ms, _condition, null, null).ConfigureAwait(false);
                        LeoTrace.WriteLine("Uploaded Single Block: " + _blob.Name);
                    }
                }
                else
                {
                    if (_offset > 0)
                    {
                        var key = GetKey(_partNumber);
                        _uploads.Add(PutBlob(key, _buffer, _offset));
                        _offset = 0;
                        _partNumber++;
                    }

                    _buffer = null;
                    await Task.WhenAll(_uploads).ConfigureAwait(false);

                    var blocks = new List<string>();
                    for (var i = 1; i < _partNumber; i++)
                    {
                        blocks.Add(GetKey(i));
                    }

                    await _blob.PutBlockListAsync(blocks).ConfigureAwait(false);
                    LeoTrace.WriteLine("Finished Put Blocks using " + _partNumber + " total blocks: " + _blob.Name);
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (!_hasCompleted)
            {
                // Run on another thread to avoid deadlock
                Task.Run(() => Complete()).Wait();
            }

            base.Dispose(disposing);
        }

        private string GetKey(int part)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(part.ToString("d7")));
        }

        private async Task PutBlob(string key, byte[] data, int length)
        {
            using (var ms = new MemoryStream(data, 0, length))
            {
                await _blob.PutBlockAsync(key, ms, null, _condition, null, null).ConfigureAwait(false);
                LeoTrace.WriteLine("Put Block '" + key + "': " + _blob.Name);
            }
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

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
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

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
