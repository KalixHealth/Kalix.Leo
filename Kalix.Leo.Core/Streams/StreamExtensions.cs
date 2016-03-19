using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    /// <summary>
    /// Extension methods for async streams
    /// </summary>
    public static class StreamExtensions
    {
        // 8k buffer size...
        private const int BufferSize = 8192;

        /// <summary>
        /// Read all the bytes from a stream
        /// </summary>
        public static async Task<byte[]> ReadBytes(this IReadAsyncStream readStream)
        {
            using (var ms = new MemoryStream())
            {
                await readStream.CopyToStream(ms, CancellationToken.None).ConfigureAwait(false);
                return ms.ToArray();
            }
        }

        /// <summary>
        /// Move all bytes from a read stream to a normal write stream 
        /// (Note: uses sync call on normal stream)
        /// </summary>
        public static async Task CopyToStream(this IReadAsyncStream readStream, Stream stream, CancellationToken ct)
        {
            // We have the double exception catching to avoid the using() from hiding the inner exception
            Exception inner = null;
            try
            {
                using (readStream)
                {
                    try
                    {
                        var buffer = new byte[BufferSize];
                        int read;
                        do
                        {
                            ct.ThrowIfCancellationRequested();
                            read = await readStream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false);
                            if (read > 0)
                            {
                                ct.ThrowIfCancellationRequested();
                                stream.Write(buffer, 0, read);
                            }
                        } while (read > 0);
                    }
                    catch (Exception e)
                    {
                        inner = e;
                        throw;
                    }
                }
            }
            catch (Exception)
            {
                if (inner != null)
                {
                    throw inner;
                }
                else throw;
            }
        }

        /// <summary>
        /// Straight copy from the reader to the writer
        /// </summary>
        public static async Task CopyToAsync(this IReadAsyncStream readStream, IWriteAsyncStream writeStream, CancellationToken ct)
        {
            // We have the double exception catching to avoid the using() from hiding the inner exception
            Exception inner = null;

            try
            {
                using (readStream)
                {
                    try
                    {
                        var buffer = new byte[BufferSize];
                        int read;
                        do
                        {
                            ct.ThrowIfCancellationRequested();
                            read = await readStream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false);
                            if (read > 0)
                            {
                                ct.ThrowIfCancellationRequested();
                                await writeStream.WriteAsync(buffer, 0, read, ct).ConfigureAwait(false);
                            }
                        } while (read > 0);
                    }
                    catch (Exception e)
                    {
                        inner = e;
                        throw;
                    }
                }
            }
            catch (Exception)
            {
                if (inner != null)
                {
                    throw inner;
                }
                else throw;
            }
        }

        /// <summary>
        /// Create a read stream that will transform any data (using streams) before returning your actual read data
        /// </summary>
        /// <param name="stream">Underlying read stream to pull data from before transforming</param>
        /// <param name="readStack">Using the passed read stream, wrap to create your data transformations</param>
        /// <returns>A read stream</returns>
        public static IReadAsyncStream AddTransformer(this IReadAsyncStream stream, Func<Stream, Stream> readStack)
        {
            return new ReadWriteAsyncStream(stream, readStack);
        }

        /// <summary>
        /// Create a write stream that will transform any data (using streams) before passing it down to the underlying stream
        /// </summary>
        /// <param name="stream">Underlying write stream to pass transformed data</param>
        /// <param name="writeStack">Using the passed write stream, wrap to create your data transformations</param>
        /// <returns>A write stream</returns>
        public static IWriteAsyncStream AddTransformer(this IWriteAsyncStream stream, Func<Stream, Stream> writeStack)
        {
            return new WriteWriteAsyncStream(stream, writeStack);
        }

        /// <summary>
        /// As you write will write data to the write stack and then save what comes out of the write stack
        /// </summary>
        private class WriteWriteAsyncStream : IWriteAsyncStream
        {
            private readonly IWriteAsyncStream _stream;
            private readonly MemoryStream _ms;
            private readonly WriteWatcherStream _watcher;
            private readonly Stream _stack;

            private bool _isComplete;

            public WriteWriteAsyncStream(IWriteAsyncStream stream, Func<Stream, Stream> writeStack)
            {
                _stream = stream;
                _ms = new MemoryStream();
                _watcher = new WriteWatcherStream(_ms, () => { });  // We do this mostly so that the ms is not disposed
                _stack = writeStack(_watcher);
            }

            public async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
            {
                if(_isComplete) { throw new InvalidOperationException("Cannot write anymore to a completed stream"); }

                ct.ThrowIfCancellationRequested();
                _stack.Write(buffer, offset, count);
                if(_ms.Length > 0)
                {
                    var data = _ms.GetBuffer(); // This does not copy the array! more efficient!
                    ct.ThrowIfCancellationRequested();
                    await _stream.WriteAsync(data, 0, (int)_ms.Length, ct).ConfigureAwait(false);
                    _ms.SetLength(0);
                }
            }

            public async Task Complete(CancellationToken ct)
            {
                if (!_isComplete)
                {
                    _stack.Dispose();
                    if (_ms.Length > 0)
                    {
                        var data = _ms.GetBuffer();
                        await _stream.WriteAsync(data, 0, (int)_ms.Length, ct).ConfigureAwait(false);
                        _ms.SetLength(0);
                    }
                    await _stream.Complete(ct).ConfigureAwait(false);
                    _isComplete = true;
                }
            }

            public Task FlushAsync(CancellationToken ct)
            {
                return Task.FromResult(0);
            }

            public void Dispose()
            {
                if (!_isComplete) { _stack.Dispose(); }
                _ms.Dispose();
                _stream.Dispose();
            }
        }

        /// <summary>
        /// As you read will prepare data for the read stack, which will then go ahead and pull it
        /// </summary>
        private class ReadWriteAsyncStream : IReadAsyncStream
        {
            private readonly IReadAsyncStream _stream;
            private readonly ReadWatcherStream _readStream;
            private readonly Stream _stack;

            public ReadWriteAsyncStream(IReadAsyncStream stream, Func<Stream, Stream> readStack)
            {
                _stream = stream;
                _readStream = new ReadWatcherStream(stream);
                _stack = readStack(_readStream);
            }

            public async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
            {
                // Don't ever load more than buffersize at a time
                count = count > BufferSize ? BufferSize : count;

                // Try and pre-load data the proper async way
                if(_readStream.NeedsData)
                {
                    await _readStream.PrepareData().ConfigureAwait(false);
                }

                return _stack.Read(buffer, offset, count);
            }

            public void Dispose()
            {
                _stack.Dispose();
                _readStream.Dispose();
                _stream.Dispose();
            }
        }

        private class ReadWatcherStream : Stream
        {
            private readonly IReadAsyncStream _stream;
            
            private readonly byte[] _buffer;
            private bool _bufferNeedsData;
            private int _offset;
            private int _length;

            private readonly byte[] _buffer2;
            private bool _bufferNeedsData2;
            private int _offset2;
            private int _length2;

            private bool _isFirstBuffer;
            private bool _isDone;

            public ReadWatcherStream(IReadAsyncStream stream)
            {
                _stream = stream;
                _buffer = new byte[BufferSize * 2]; // We make larger buffer sizes to try to avoid extra data pull 
                _buffer2 = new byte[BufferSize * 2];

                _bufferNeedsData = true;
                _bufferNeedsData2 = true;
                _isFirstBuffer = true;
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                int read = 0;
                int currentCount = count;
                while (currentCount > 0)
                {
                    // We are completely done
                    if(_isDone && _bufferNeedsData && _bufferNeedsData2)
                    {
                        return read;
                    }

                    // We ran out of read data...
                    if(!_isDone && _bufferNeedsData && _bufferNeedsData2)
                    {
                        // If we have read something, just return that for now
                        if(read > 0) { return read; }

                        // Otherwise we are forced to load more data :(
                        // Note: this could cause deadlock? Shouldn't in Leo Engine...
                        PrepareData().Wait();
                        continue;
                    }

                    if(_isFirstBuffer)
                    {
                        if (_bufferNeedsData)
                        {
                            _isFirstBuffer = false;
                        }
                        else
                        {
                            var toRead = Math.Min(currentCount, _length - _offset);
                            Buffer.BlockCopy(_buffer, _offset, buffer, offset, toRead);
                            offset += toRead;
                            _offset += toRead;
                            read += toRead;
                            currentCount -= toRead;
                            if(_offset == _length)
                            {
                                _bufferNeedsData = true;
                                _isFirstBuffer = _bufferNeedsData2; // Only flip buffers if we actually have data in the other buffer
                            }
                        }
                    }
                    else
                    {
                        if(_bufferNeedsData2)
                        {
                            _isFirstBuffer = true;
                        }
                        else
                        {
                            var toRead = Math.Min(currentCount, _length2 - _offset2);
                            Buffer.BlockCopy(_buffer2, _offset2, buffer, offset, toRead);
                            offset += toRead;
                            _offset2 += toRead;
                            read += toRead;
                            currentCount -= toRead;
                            if (_offset2 == _length2)
                            {
                                _bufferNeedsData2 = true;
                                _isFirstBuffer = true;
                            }
                        }
                    }
                }
                return read;
            }

            public async Task PrepareData()
            {
                if(!_isDone && _bufferNeedsData)
                {
                    _length = await _stream.ReadAsync(_buffer, 0, _buffer.Length, CancellationToken.None).ConfigureAwait(false);
                    if(_length == 0)
                    {
                        _isDone = true;
                        return;
                    }
                    _offset = 0;
                    _bufferNeedsData = false;
                }

                if (!_isDone && _bufferNeedsData2)
                {
                    _length2 = await _stream.ReadAsync(_buffer2, 0, _buffer2.Length, CancellationToken.None).ConfigureAwait(false);
                    if (_length2 == 0)
                    {
                        _isDone = true;
                        return;
                    }
                    _offset2 = 0;
                    _bufferNeedsData2 = false;
                }
            }

            public bool NeedsData
            {
                get
                {
                    return _bufferNeedsData || _bufferNeedsData2;
                }
            }

            public override bool CanRead
            {
                get
                {
                    return true;
                }
            }

            public override bool CanSeek
            {
                get
                {
                    return false;
                }
            }

            public override bool CanWrite
            {
                get
                {
                    return false;
                }
            }

            public override long Length
            {
                get
                {
                    throw new NotImplementedException();
                }
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

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }

            public override void Flush()
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

        private class WriteWatcherStream : Stream
        {
            private readonly Stream _stream;
            private readonly Action _writeAction;

            public WriteWatcherStream(Stream stream, Action writeAction)
            {
                _stream = stream;
                _writeAction = writeAction;
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                // This does happen in the crypto stream! Sometimes just a 0 count write????
                if (count > 0)
                {
                    _stream.Write(buffer, offset, count);
                    _writeAction();
                }
            }

            public override void Flush()
            {
            }

            public override bool CanRead
            {
                get
                {
                    return false;
                }
            }

            public override bool CanSeek
            {
                get
                {
                    return false;
                }
            }

            public override bool CanWrite
            {
                get
                {
                    return true;
                }
            }

            public override long Length
            {
                get
                {
                    throw new NotImplementedException();
                }
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
}
