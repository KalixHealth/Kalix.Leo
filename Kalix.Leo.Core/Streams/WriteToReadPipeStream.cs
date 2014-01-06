using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Streams
{
    public class WriteToReadPipeStream : Stream
    {
        private const int KB = 1024;
        private const int MB = 1024 * KB;

        private readonly byte[] _buffer;
        private readonly CancellationTokenSource _disposeToken;
        private readonly object _readLock = new object();
        private readonly object _writeLock = new object();

        private CancellationTokenSource _readSource;
        private CancellationTokenSource _writeSource;
        private bool _isComplete;
        private bool _isDisposed;
        private long _readPosition;
        private long _writePosition;

        public WriteToReadPipeStream(int bufferSize = 8 * MB)
        {
            _buffer = new byte[bufferSize];
            _disposeToken = new CancellationTokenSource();
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
            throw new NotSupportedException("Only task async methods are supported in this stream");
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            // noop so just return an already completed task...
            return Task.FromResult(0);
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
            throw new NotSupportedException("Only task async methods are supported in this stream");
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            // Create a linked token that listens for dispose as well
            cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, cancellationToken).Token;

            var dataToRead = await LockBlocksToRead(count, cancellationToken);
            if (dataToRead == 0) { return 0; }
            
            var realReadPosition = (int)(_readPosition % _buffer.Length);
            var realEndReadPosition = (int)((_readPosition + dataToRead) % _buffer.Length);

            if (realEndReadPosition <= realReadPosition)
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

            ReleaseBlocksToWrite(dataToRead);

            // note: dataToRead could be less than count... but this is allowed
            // it does not mean that is the rest of the data...
            return dataToRead;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("Only task async methods are supported in this stream");
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            // Create a linked token that listens for dispose as well
            cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, cancellationToken).Token;

            while (count > 0)
            {
                var dataToWrite = await LockBlocksToWrite(count, cancellationToken);
                var realWritePosition = (int)(_writePosition % _buffer.Length);
                var realEndWritePosition = (int)((_writePosition + dataToWrite) % _buffer.Length);

                if (realEndWritePosition <= realWritePosition)
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

                ReleaseBlocksToRead(dataToWrite);
                offset += dataToWrite;
                count -= dataToWrite;
            }
        }

        public void FinishWriting()
        {
            if (!_isComplete)
            {
                _isComplete = true;
                ReleaseBlocksToRead(0);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                _disposeToken.Cancel();
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

        private void ReleaseBlocksToRead(int count)
        {
            lock (_readLock)
            {
                _writePosition += count;
                if (_readSource != null)
                {
                    var temp = _readSource;
                    _readSource = null;
                    temp.Cancel();
                }
            }
        }

        // These are the Thread sensitive areas of the stream
        private async Task<int> LockBlocksToRead(int maxCount, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && !_isComplete && _writePosition == _readPosition)
            {
                Task waitTask;

                lock (_readLock)
                {
                    if (_readSource != null)
                    {
                        throw new InvalidOperationException("Internal read source is not null, this would only happen if you are reading with more than one thread (don't do it!)");
                    }

                    _readSource = new CancellationTokenSource();
                    var waitToken = CancellationTokenSource.CreateLinkedTokenSource(_readSource.Token, cancellationToken).Token;

                    // Create a task that will simply finish when cancellation happens
                    var tcs = new TaskCompletionSource<bool>();
                    waitToken.Register(() => tcs.TrySetResult(true), useSynchronizationContext: false);
                    waitTask = tcs.Task;

                    // Double check the cancellation token after the register call
                    if (cancellationToken.IsCancellationRequested) { return 0; }
                }

                await waitTask;
            }

            if (cancellationToken.IsCancellationRequested || _writePosition == _readPosition) { return 0; }

            return (int)(_writePosition - _readPosition > maxCount ? maxCount : _writePosition - _readPosition);
        }

        private void ReleaseBlocksToWrite(int count)
        {
            lock (_writeLock)
            {
                _readPosition += count;
                if (_writeSource != null)
                {
                    var temp = _writeSource;
                    _writeSource = null;
                    temp.Cancel();
                }
            }
        }

        private async Task<int> LockBlocksToWrite(int maxCount, CancellationToken cancellationToken)
        {
            // We do not check for is complete in this case, because we might still be waiting
            // to finish writing the last couple bytes
            while (!_isDisposed && (_writePosition - _readPosition) >= _buffer.Length)
            {
                Task waitTask;

                lock (_writeLock)
                {
                    if (_writeSource != null)
                    {
                        throw new InvalidOperationException("Internal write source is not null, this would only happen if you are writing with more than one thread (don't do it!)");
                    }

                    _writeSource = new CancellationTokenSource();
                    var waitToken = CancellationTokenSource.CreateLinkedTokenSource(_writeSource.Token, cancellationToken).Token;

                    // Create a task that will simply finish when cancellation happens
                    var tcs = new TaskCompletionSource<bool>();
                    waitToken.Register(() => tcs.TrySetResult(true), useSynchronizationContext: false);
                    waitTask = tcs.Task;

                    // Double check the cancellation token after the register call
                    if (cancellationToken.IsCancellationRequested) { return 0; }
                }

                await waitTask;
            }

            if (_isDisposed) { return 0; }

            var dataToWrite = (int)(_buffer.Length - _writePosition + _readPosition);
            if (dataToWrite > maxCount)
            {
                dataToWrite = maxCount;
            }
            return dataToWrite;
        }
    }
}
