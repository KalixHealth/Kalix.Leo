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
    public class AzureWriteBlockBlobStream : IWriteAsyncStream
    {
        private const int AzureBlockSize = 4194304;

        private readonly CloudBlockBlob _blob;
        private readonly AccessCondition _condition;

        private MemoryStream _buff;
        private int _partNumber;
        private long _length;

        private bool _hasCompleted;

        public AzureWriteBlockBlobStream(CloudBlockBlob blob, AccessCondition condition)
        {
            _blob = blob;
            _condition = condition;

            long min = Math.Min(blob.Properties.Length, AzureBlockSize);
            if (min < 0) { min = 0; }
            _buff = new MemoryStream((int)min);
            _partNumber = 1;
        }

        public async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            if (_hasCompleted)
            {
                throw new InvalidOperationException("The azure write stream has already been completed");
            }

            _length += count;
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
                    await PutBlobAsync(key, data, (int)_buff.Length, ct).ConfigureAwait(false);

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
                    await _blob.UploadFromByteArrayAsync(data, 0, length, _condition, null, null, ct).ConfigureAwait(false);
                    LeoTrace.WriteLine("Uploaded Single Block: " + _blob.Name);
                }
                else
                {
                    if (length > 0)
                    {
                        var key = GetKey(_partNumber);
                        await PutBlobAsync(key, data, length, ct).ConfigureAwait(false);
                        _partNumber++;
                    }
                    
                    var blocks = new List<string>();
                    for (var i = 1; i < _partNumber; i++)
                    {
                        blocks.Add(GetKey(i));
                    }

                    await _blob.PutBlockListAsync(blocks, ct).ConfigureAwait(false);
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
            return Task.FromResult(0);
        }

        public Task FlushAsync(CancellationToken ct)
        {
            return Task.FromResult(0);
        }

        public void Dispose()
        {
            _buff.Dispose();
            _hasCompleted = true;
        }

        private string GetKey(int part)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(part.ToString("d7")));
        }

        private async Task PutBlobAsync(string key, byte[] data, int length, CancellationToken ct)
        {
            using (var ms = new MemoryStream(data, 0, length))
            {
                await _blob.PutBlockAsync(key, ms, null, _condition, null, null, ct).ConfigureAwait(false);
            }
        }
    }
}
