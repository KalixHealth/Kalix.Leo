using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage
{
    public class AzureReadBlockBlobStream : IReadAsyncStream
    {
        private const int AzureBlockSize = 4194304;
        private readonly CloudBlockBlob _blob;
        private readonly bool _needsToReadBlockList;

        private int _currentBlock;
        private List<ListBlockItem> _orderedBlocks;
        private MemoryStream _currentBlockData;

        private int _offset;
        private long _position;

        public AzureReadBlockBlobStream(CloudBlockBlob blob, bool needsToReadBlockList)
        {
            _blob = blob;
            _needsToReadBlockList = needsToReadBlockList;
            _orderedBlocks = null;
            var min = Math.Min(blob.Properties.Length, AzureBlockSize);
            if(min < 0) { min = 0; }
            _currentBlockData = new MemoryStream((int)min);
            _position = 0;
        }

        public async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            if (_needsToReadBlockList && _orderedBlocks == null)
            {
                await GetBlocksAsync(ct).ConfigureAwait(false);
            }

            if (_currentBlockData.Length == 0)
            {
                await GetNextChunkOfDataAsync(ct).ConfigureAwait(false);
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
            var blockList = (await _blob.DownloadBlockListAsync(ct).ConfigureAwait(false)).ToList();
            // Make sure that we get the blocks in order...
            _orderedBlocks = blockList.OrderBy(l => BitConverter.ToInt32(Convert.FromBase64String(l.Name), 0)).ToList();
            _currentBlock = 0;
        }

        private async Task GetNextChunkOfDataAsync(CancellationToken ct)
        {
            var total = _needsToReadBlockList ? _orderedBlocks.Count : _blob.Properties.Length;
            var current = _needsToReadBlockList ? _currentBlock : _position;
            
            if (total == 0)
            {
                if (current != 0) { return; }

                // Doesn't have blocks - just do the single download
                await _blob.DownloadToStreamAsync(_currentBlockData, ct).ConfigureAwait(false);
            }
            else
            {
                if (current >= total) { return; }

                // Has blocks, work out the data from the current chunk
                // Do not assume each block is uniform... Make sure to use length info of previous blocks
                var start = _needsToReadBlockList ? _orderedBlocks.TakeWhile((ol, i) => i < _currentBlock).Sum(ol => ol.Length) : current;
                var length = _needsToReadBlockList ? _orderedBlocks[_currentBlock].Length : Math.Min(AzureBlockSize, total - current);
                await _blob.DownloadRangeToStreamAsync(_currentBlockData, start, length, ct).ConfigureAwait(false);
            }

            _currentBlock++;
        }

        public void Dispose()
        {
            _currentBlockData.Dispose();
        }
    }
}
