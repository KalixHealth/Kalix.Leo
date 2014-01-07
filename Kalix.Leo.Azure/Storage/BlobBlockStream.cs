using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
        private readonly AccessCondition _condition;
        private readonly List<string> _blocks;
        private readonly int _parallelBlocks;

        private byte[] _internalBuffer;
        private int _currentBlock;
        private int _currentPosition;
        private bool _isClosed;
        private List<Task> _tasks;

        /// <summary>
        /// Create a stream on top of a cloud blob
        /// </summary>
        /// <param name="blob">blob that the stream will write to in chunks</param>
        /// <param name="context">context to run in, can be null</param>
        public BlobBlockStream(CloudBlockBlob blob, OperationContext context, AccessCondition condition, int blocksInParallel = 4)
        {
            _blob = blob;
            _context = context;
            _condition = condition;
            _currentBlock = 0;
            _internalBuffer = new byte[MAXBLOCKSIZE];
            _currentPosition = 0;
            _blocks = new List<string>();
            _tasks = new List<Task>();
            _parallelBlocks = blocksInParallel;
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
            throw new NotSupportedException("Only task async methods are supported in this stream");
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Stream is closed");
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
                    // Make sure we are not loading too many blocks
                    // at the same time (and hogging memory!)
                    if (_tasks.Count >= _parallelBlocks)
                    {
                        await Task.WhenAny(_tasks);
                        _tasks = _tasks.Where(t => !t.IsCompleted).ToList();
                    }

                    string blockId = GetBlockIdBase64(_currentBlock);

                    var task = _blob.PutBlockAsync(blockId, new MemoryStream(_internalBuffer), null, _condition, null, _context);

                    _blocks.Add(blockId);
                    _tasks.Add(task);

                    _currentBlock++;
                    _currentPosition = 0;
                    _internalBuffer = new byte[MAXBLOCKSIZE];
                }

                count = count - chunk;
                offset = offset + chunk;
            }
        }

        public override void Flush()
        {
            throw new NotSupportedException("Only task async methods are supported in this stream");
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
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
                        _blob.PutBlock(blockId, ms, null, _condition, null, _context);
                    }

                    _blocks.Add(blockId);

                    _currentBlock++;
                    _currentPosition = 0;
                }

                if (_blocks.Any())
                {
                    // Make sure all our blocks finish
                    if (_tasks.Any())
                    {
                        Task.WaitAll(_tasks.ToArray());
                    }

                    _blob.PutBlockList(_blocks, _condition, null, _context);
                    _blocks.Clear();
                }
                _isClosed = true;
            }
        }

        private static string GetBlockIdBase64(int block)
        {
            return Convert.ToBase64String(BitConverter.GetBytes(block));
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
