using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage
{
    public sealed class AzureReadBlockBlobStream : IReadAsyncStream
    {
        private const int AzureBlockSize = 4194304;
        private readonly BlockBlobClient _blob;
        private readonly bool _needsToReadBlockList;

        private int _currentBlock;
        private List<BlobBlock> _orderedBlocks;
        private MemoryStream _currentBlockData;

        private readonly long _contentLength;
        private int _offset;
        private long _position;

        public AzureReadBlockBlobStream(BlockBlobClient blob, long contentLength, bool needsToReadBlockList)
        {
            _blob = blob;
            _contentLength = contentLength;
            _needsToReadBlockList = needsToReadBlockList;
            _orderedBlocks = null;
            var min = Math.Min(contentLength, AzureBlockSize);
            if(min < 0) { min = 0; }
            _currentBlockData = new MemoryStream((int)min);
            _position = 0;
        }

        public async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            if (_needsToReadBlockList && _orderedBlocks == null)
            {
                await GetBlocksAsync(ct);
            }

            if (_currentBlockData.Length == 0)
            {
                await GetNextChunkOfDataAsync(ct);
                if (_currentBlockData.Length == 0) { return 0; }
            }

            var length = Math.Min((int)_currentBlockData.Length - _offset, count);
            if (length > 0)
            {
                var currentBlockData = _currentBlockData.GetBuffer();
                ct.ThrowIfCancellationRequested();
                Buffer.BlockCopy(currentBlockData, _offset, buffer, offset, length);
                _offset += length;
                _position += length;
                if (_offset == _currentBlockData.Length)
                {
                    _offset = 0;
                    _currentBlockData.SetLength(0);
                }
            }

            return length;
        }

        private async Task GetBlocksAsync(CancellationToken ct)
        {
            BlockList blockList = await _blob.GetBlockListAsync(BlockListTypes.Committed, cancellationToken: ct);
            // Make sure that we get the blocks in order...
            _orderedBlocks = blockList.CommittedBlocks.OrderBy(l => BitConverter.ToInt32(Convert.FromBase64String(l.Name), 0)).ToList();
            _currentBlock = 0;
        }

        private async Task GetNextChunkOfDataAsync(CancellationToken ct)
        {
            var total = _needsToReadBlockList ? _orderedBlocks.Count : _contentLength;
            var current = _needsToReadBlockList ? _currentBlock : _position;

            if (total == 0)
            {
                if (current != 0) { return; }

                // Doesn't have blocks - just do the single download
                await _blob.DownloadToAsync(_currentBlockData, ct);
            }
            else
            {
                if (current >= total) { return; }

                // Has blocks, work out the data from the current chunk
                // Do not assume each block is uniform... Make sure to use length info of previous blocks
                var start = _needsToReadBlockList ? _orderedBlocks.TakeWhile((ol, i) => i < _currentBlock).Sum(ol => ol.Size) : current;
                var length = _needsToReadBlockList ? _orderedBlocks[_currentBlock].Size : Math.Min(AzureBlockSize, total - current);
                using (BlobDownloadInfo info = await _blob.DownloadAsync(new HttpRange(start, length), cancellationToken: ct))
                {
                    await info.Content.CopyToAsync(_currentBlockData, ct);
                }
            }

            _currentBlock++;
        }

        public void Dispose()
        {
            _currentBlockData.Dispose();
        }
    }
}
