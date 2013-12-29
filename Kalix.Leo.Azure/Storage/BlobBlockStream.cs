using Microsoft.WindowsAzure.Storage;
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
        private const long KB = 1024;
        private const long MB = 1024 * KB;
        private const long GB = 1024 * MB;
        //private const long MAXBLOCKS = 50000;
        //private const long MAXBLOBSIZE = 200 * GB;
        private const long MAXBLOCKSIZE = 4 * MB;

        private readonly CloudBlockBlob _blob;
        private readonly OperationContext _context;
        private readonly byte[] _internalBuffer;
        private readonly List<string> _blocks;

        private int _currentBlock;
        private int _currentPosition;
        private bool _isClosed;

        /// <summary>
        /// Create a stream on top of a cloud blob
        /// </summary>
        /// <param name="blob">blob that the stream will write to in chunks</param>
        /// <param name="context">context to run in, can be null</param>
        public BlobBlockStream(CloudBlockBlob blob, OperationContext context)
        {
            _blob = blob;
            _context = context;
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
                        _blob.PutBlock(blockId, ms, null, null, null, _context);
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
                        _blob.PutBlock(blockId, ms, null, null, null, _context);
                    }

                    _blocks.Add(blockId);

                    _currentBlock++;
                    _currentPosition = 0;
                }

                if (_blocks.Any())
                {
                    _blob.PutBlockList(_blocks, null, null, _context);
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
