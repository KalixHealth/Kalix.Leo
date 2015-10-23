using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage
{
    public class AzureReadBlockBlobStream : Stream
    {
        private const int AzureBlockSize = 4194304;
        private readonly CloudBlockBlob _blob;
        private readonly bool _needsToReadBlockList;

        private int _currentBlock;
        private List<ListBlockItem> _orderedBlocks;
        private byte[] _currentBlockData;

        private int _offset;
        private long _position;

        public AzureReadBlockBlobStream(CloudBlockBlob blob, bool needsToReadBlockList)
        {
            _blob = blob;
            _needsToReadBlockList = needsToReadBlockList;
            _orderedBlocks = null;
            _currentBlockData = null;
            _position = 0;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_needsToReadBlockList && _orderedBlocks == null)
            {
                GetBlocks();
            }

            if(_currentBlockData == null)
            {
                _currentBlockData = GetNextChunkOfData();
                if (_currentBlockData == null) { return 0; }
            }

            var length = Math.Min(_currentBlockData.Length - _offset, count);
            if (length > 0)
            {
                Buffer.BlockCopy(_currentBlockData, _offset, buffer, offset, length);
                _offset += length;
                _position += length;
                if(_offset == _currentBlockData.Length)
                {
                    _offset = 0;
                    _currentBlockData = null;
                }
            }

            return length;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_needsToReadBlockList && _orderedBlocks == null)
            {
                await GetBlocksAsync().ConfigureAwait(false);
            }

            if (_currentBlockData == null)
            {
                _currentBlockData = await GetNextChunkOfDataAsync().ConfigureAwait(false);
                if (_currentBlockData == null) { return 0; }
            }

            var length = Math.Min(_currentBlockData.Length - _offset, count);
            if (length > 0)
            {
                Buffer.BlockCopy(_currentBlockData, _offset, buffer, offset, length);
                _offset += length;
                _position += length;
                if (_offset == _currentBlockData.Length)
                {
                    _offset = 0;
                    _currentBlockData = null;
                }
            }

            return length;
        }

        private void GetBlocks()
        {
            var blockList = _blob.DownloadBlockList().ToList();
            // Make sure that we get the blocks in order...
            _orderedBlocks = blockList.OrderBy(l => BitConverter.ToInt32(Convert.FromBase64String(l.Name), 0)).ToList();
            _currentBlock = 0;
        }

        private async Task GetBlocksAsync()
        {
            var blockList = (await _blob.DownloadBlockListAsync().ConfigureAwait(false)).ToList();
            // Make sure that we get the blocks in order...
            _orderedBlocks = blockList.OrderBy(l => BitConverter.ToInt32(Convert.FromBase64String(l.Name), 0)).ToList();
            _currentBlock = 0;
        }

        private byte[] GetNextChunkOfData()
        {
            var total = _needsToReadBlockList ? _orderedBlocks.Count : _blob.Properties.Length;
            var current = _needsToReadBlockList ? _currentBlock : _position;
            
            byte[] data;
            if(total == 0)
            {
                if(current != 0) { return null; }

                // Doesn't have blocks - just do the single download
                using (var ms = new MemoryStream())
                {
                    _blob.DownloadToStream(ms);
                    data = ms.ToArray();
                }
            }
            else
            {
                if (current >= total) { return null; }

                // Has blocks, work out the data from the current chunk
                // Do not assume each block is uniform... Make sure to use length info of previous blocks
                var start = _needsToReadBlockList ? _orderedBlocks.TakeWhile((ol, i) => i < _currentBlock).Sum(ol => ol.Length) : current;
                var length = _needsToReadBlockList ? _orderedBlocks[_currentBlock].Length : Math.Min(AzureBlockSize, total - current);
                using (var ms = new MemoryStream())
                {
                    _blob.DownloadRangeToStream(ms, start, length);
                    data = ms.ToArray();
                }
            }

            _currentBlock++;
            return data;
        }

        private async Task<byte[]> GetNextChunkOfDataAsync()
        {
            var total = _needsToReadBlockList ? _orderedBlocks.Count : _blob.Properties.Length;
            var current = _needsToReadBlockList ? _currentBlock : _position;

            byte[] data;
            if (total == 0)
            {
                if (current != 0) { return null; }

                // Doesn't have blocks - just do the single download
                using (var ms = new MemoryStream())
                {
                    await _blob.DownloadToStreamAsync(ms).ConfigureAwait(false);
                    data = ms.ToArray();
                }
            }
            else
            {
                if (current >= total) { return null; }

                // Has blocks, work out the data from the current chunk
                // Do not assume each block is uniform... Make sure to use length info of previous blocks
                var start = _needsToReadBlockList ? _orderedBlocks.TakeWhile((ol, i) => i < _currentBlock).Sum(ol => ol.Length) : current;
                var length = _needsToReadBlockList ? _orderedBlocks[_currentBlock].Length : Math.Min(AzureBlockSize, total - current);
                using (var ms = new MemoryStream())
                {
                    await _blob.DownloadRangeToStreamAsync(ms, start, length).ConfigureAwait(false);
                    data = ms.ToArray();
                }
            }

            _currentBlock++;
            return data;
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override long Length
        {
            get { throw new NotImplementedException(); }
        }

        public override long Position
        {
            get
            {
                return _position;
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

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
