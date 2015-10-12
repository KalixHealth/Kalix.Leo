using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kalix.Leo.Azure.Storage
{
    public class AzureReadBlockBlobStream : Stream
    {
        private readonly CloudBlockBlob _blob;
        
        private int _currentBlock;
        private List<ListBlockItem> _orderedBlocks;
        private byte[] _currentBlockData;

        private int _offset;
        private long _position;

        public AzureReadBlockBlobStream(CloudBlockBlob blob)
        {
            _blob = blob;
            _orderedBlocks = null;
            _currentBlockData = null;
            _position = 0;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_orderedBlocks == null)
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

        private void GetBlocks()
        {
            var blockList = _blob.DownloadBlockList().ToList();
            // Make sure that we get the blocks in order...
            _orderedBlocks = blockList.OrderBy(l => BitConverter.ToInt32(Convert.FromBase64String(l.Name), 0)).ToList();
            _currentBlock = 0;
        }

        private byte[] GetNextChunkOfData()
        {
            var total = _orderedBlocks.Count;
            
            byte[] data;
            if(total == 0)
            {
                if(_currentBlock != 0) { return null; }

                // Doesn't have blocks - just do the single download
                using (var ms = new MemoryStream())
                {
                    _blob.DownloadToStream(ms);
                    data = ms.ToArray();
                }
            }
            else
            {
                if (_currentBlock >= total) { return null; }

                // Has blocks, work out the data from the current chunk
                // Do not assume each block is uniform... Make sure to use length info of previous blocks
                var start = _orderedBlocks.TakeWhile((ol, i) => i < _currentBlock).Sum(ol => ol.Length);
                var length = _orderedBlocks[_currentBlock].Length;
                using (var ms = new MemoryStream())
                {
                    _blob.DownloadRangeToStream(ms, start, length);
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
