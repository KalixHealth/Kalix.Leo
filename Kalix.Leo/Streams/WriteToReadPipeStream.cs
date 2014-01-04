using System;
using System.IO;
using System.Threading;

namespace Kalix.Leo.Streams
{
    public class WriteToReadPipeStream : Stream
    {
        private readonly byte[] _buffer;
        private readonly object _readLock = new object();
        private readonly object _writeLock = new object();

        private bool _isComplete;
        private bool _isDisposed;
        private long _readPosition;
        private long _writePosition;

        public WriteToReadPipeStream(int bufferSize = 4096)
        {
            _buffer = new byte[bufferSize];
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
            get { return true; }
        }

        public override void Flush()
        {
            // Do not need to do anything here...
        }

        public override long Position
        {
            get
            {
                return _readPosition;
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            lock (_readLock)
            {
                while (!_isDisposed && !_isComplete && _writePosition == _readPosition)
                {
                    Monitor.Wait(_readLock);
                }

                if (_isDisposed || _writePosition == _readPosition) { return 0; }

                var dataToRead = (int)(_writePosition - _readPosition > count ? count : _writePosition - _readPosition);
                var realReadPosition = (int)(_readPosition % _buffer.Length);
                var realEndReadPosition = (int)((_readPosition + dataToRead) % _buffer.Length);

                if (realEndReadPosition < realReadPosition)
                {
                    // We have to loop around
                    var initialBlock = _buffer.Length - realReadPosition;
                    Buffer.BlockCopy(_buffer, realReadPosition, buffer, offset, initialBlock);
                    Buffer.BlockCopy(_buffer, 0, buffer, offset + initialBlock, dataToRead - initialBlock);
                }
                else
                {
                    // We can write the whole lot at once
                    Buffer.BlockCopy(_buffer, realReadPosition, buffer, offset, dataToRead);
                }

                _readPosition += dataToRead;

                // Let a waiting write thread know that they can do some writing now (as buffer memory has been freed)
                Monitor.Pulse(_writeLock);

                // note: dataToRead could be less than count... but this is allowed
                // it does not mean that is the rest of the data...
                return dataToRead;
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            lock (_writeLock)
            {
                while (count > 0)
                {
                    // We do not check for is complete in this case, because we might still be waiting
                    // to finish writing the last couple bytes
                    while (!_isDisposed && (_writePosition - _readPosition) >= _buffer.Length)
                    {
                        Monitor.Wait(_writeLock);
                    }

                    if (_isDisposed) { return; }

                    var dataToWrite = (int)(_buffer.Length - _writePosition + _readPosition);
                    if (dataToWrite > count)
                    {
                        dataToWrite = count;
                    }

                    var realWritePosition = (int)(_writePosition % _buffer.Length);
                    var realEndWritePosition = (int)((_writePosition + dataToWrite) % _buffer.Length);

                    if (realEndWritePosition < realWritePosition)
                    {
                        // We have to loop around
                        var initialBlock = _buffer.Length - realWritePosition;
                        Buffer.BlockCopy(buffer, offset, _buffer, realWritePosition, initialBlock);
                        Buffer.BlockCopy(buffer, offset + initialBlock, _buffer, 0, dataToWrite - initialBlock);
                    }
                    else
                    {
                        // We can write the whole lot at once
                        Buffer.BlockCopy(buffer, offset, _buffer, realWritePosition, dataToWrite);
                    }

                    _writePosition += dataToWrite;
                    offset += dataToWrite;
                    count -= dataToWrite;

                    // Let a waiting read thread know that they can do some reading now (as buffer memory has been written to)
                    Monitor.Pulse(_readLock);
                }
            }
        }

        public void FinishWriting()
        {
            if (!_isComplete)
            {
                _isComplete = true;
                Monitor.PulseAll(_readLock);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                FinishWriting();
                Monitor.PulseAll(_writeLock);
            }

            base.Dispose(disposing);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override long Length
        {
            get { throw new NotImplementedException(); }
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }
    }
}
