using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kalix.Leo.Azure.Storage
{
    /// <summary>
    /// A write stream that pushes data to a blob in blocks
    /// When the Close function is closed the block list is uploaded and the blob finalised
    /// </summary>
    public class BlobBlockStream : Stream
    {
        const long KB = 1024;
        const long MB = 1024 * KB;
        const long GB = 1024 * MB;
        //const long MAXBLOCKS = 50000;
        //const long MAXBLOBSIZE = 200 * GB;
        const long MAXBLOCKSIZE = 4 * MB;

        private readonly CloudBlockBlob _blob;
        private readonly byte[] _internalBuffer;
        private readonly List<string> _blocks;

        private int _currentBlock;
        private int _currentPosition;
        private bool _isClosed;

        public BlobBlockStream(CloudBlockBlob blob)
        {
            _blob = blob;
            _currentBlock = 0;
            _internalBuffer = new byte[MAXBLOCKSIZE];
            _currentPosition = 0;
            _blocks = new List<string>();
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

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException();
            }

            // This loop may or may not write an actual block
            while (count > 0)
            {
                var chunk = count;
                if (chunk > _internalBuffer.Length - _currentPosition)
                {
                    chunk = _internalBuffer.Length - _currentPosition;
                }

                for (int i = offset; i < offset + chunk; i++)
                {
                    _internalBuffer[_currentPosition] = buffer[i];
                    _currentPosition++;
                }

                if (_currentPosition == _internalBuffer.Length)
                {
                    string blockId = GetBlockIdBase64(_currentBlock);
                    using (var ms = new MemoryStream(_internalBuffer))
                    {
                        _blob.PutBlock(blockId, ms, null);
                    }

                    _blocks.Add(blockId);

                    _currentBlock++;
                    _currentPosition = 0;
                }

                count = count - chunk;
                offset = offset + chunk;
            }
        }

        public override void Flush()
        {
            // Do nothing.... Only clear the blob on close
        }

        public override void Close()
        {
            if (!_isClosed)
            {
                // If we actually created anything push it all up!
                if (_currentPosition > 0)
                {
                    string blockId = GetBlockIdBase64(_currentBlock);
                    using (var ms = new MemoryStream(_internalBuffer, 0, _currentPosition))
                    {
                        _blob.PutBlock(blockId, ms, null);
                    }

                    _blocks.Add(blockId);

                    _currentBlock++;
                    _currentPosition = 0;
                }

                if (_blocks.Any())
                {
                    _blob.PutBlockList(_blocks);
                    _blocks.Clear();
                }
                _isClosed = true;
            }
        }

        private static string GetBlockIdBase64(int block)
        {
            return Convert.ToBase64String(System.BitConverter.GetBytes(block));
        }

        //************************************************
        // NOT IMPLEMENTED STUFF
        //***********************************************
        public override long Length
        {
            get { throw new NotImplementedException(); }
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
