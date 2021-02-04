using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
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

        private readonly BlockBlobClient _blob;
        private readonly BlobRequestConditions _condition;
        private readonly IDictionary<string, string> _metadata;

        private readonly MemoryStream _buff;
        private int _partNumber;

        private bool _hasCompleted;

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotImplementedException();

        public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public AzureWriteBlockBlobStream(BlockBlobClient blob, BlobRequestConditions condition, IDictionary<string, string> metadata)
        {
            _blob = blob;
            _condition = condition;
            _metadata = metadata;

            _buff = new MemoryStream();
            _partNumber = 1;
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            if (_hasCompleted)
            {
                throw new InvalidOperationException("The azure write stream has already been completed");
            }

            while (count > 0)
            {
                var read = (int)Math.Min(AzureBlockSize - _buff.Length, count);
                _buff.Write(buffer, offset, read);
                
                count -= read;
                offset += read;
                
                if (_buff.Length == AzureBlockSize)
                {
                    var data = _buff.GetBuffer();
                    var key = GetKey(_partNumber);
                    await PutBlobAsync(key, data, (int)_buff.Length, ct);

                    _buff.SetLength(0);
                    _partNumber++;
                }
            }
        }

        public async Task Complete(CancellationToken ct)
        {
            if (!_hasCompleted)
            {
                _hasCompleted = true;

                var data = _buff.GetBuffer();
                var length = (int)_buff.Length;
                if (_partNumber == 1)
                {
                    // We haven't even uploaded one block yet... just upload it straight...
                    using (var ms = new MemoryStream(data, 0, length, false))
                    {
                        await _blob.UploadAsync(ms, new BlobUploadOptions { Conditions = _condition, Metadata = _metadata }, ct);
                    }
                    LeoTrace.WriteLine("Uploaded Single Block: " + _blob.Name);
                }
                else
                {
                    if (length > 0)
                    {
                        var key = GetKey(_partNumber);
                        await PutBlobAsync(key, data, length, ct);
                        _partNumber++;
                    }
                    
                    var blocks = new List<string>();
                    for (var i = 1; i < _partNumber; i++)
                    {
                        blocks.Add(GetKey(i));
                    }

                    await _blob.CommitBlockListAsync(blocks, new CommitBlockListOptions { Conditions = _condition, Metadata = _metadata }, ct);
                    LeoTrace.WriteLine("Finished Put Blocks using " + _partNumber + " total blocks: " + _blob.Name);
                }
                _buff.SetLength(0);
            }
        }

        public Task Cancel()
        {
            // To cancel the upload we just need to prevent the complete method from firing
            // So either the single block or PutBlockList is never called
            _hasCompleted = true;
            return Task.CompletedTask;
        }

        public override Task FlushAsync(CancellationToken ct)
        {
            return Task.CompletedTask;
        }

        protected override void Dispose(bool disposing)
        {
            _hasCompleted = true;
            if (disposing)
            {
                _buff.Dispose();
            }
            base.Dispose(disposing);
        }

        private async Task PutBlobAsync(string key, byte[] data, int length, CancellationToken ct)
        {
            using var ms = new MemoryStream(data, 0, length, false);
            await _blob.StageBlockAsync(key, ms, conditions: _condition, cancellationToken: ct);
        }

        public override void Flush()
        {
            throw new NotImplementedException();
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

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        private static string GetKey(int part)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(part.ToString("d7")));
        }
    }
}
