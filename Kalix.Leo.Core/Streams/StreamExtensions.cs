using Kalix.Leo.Streams;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace System.IO
{
    public static class StreamExtensions
    {
        /// <summary>
        /// This forces the output stream to at least have the number of bytesPerPacket specified
        /// If strict, will force the max number of bytes to also be bytesPerPacket
        /// Note: The last packet of data may be less that the required bytes size
        /// </summary>
        /// <param name="stream">Byte stream to normalise</param>
        /// <param name="bytesPerPacket">Required bytes per packet</param>
        /// <param name="isStrict">If true, will set the max buffer size to the bytes, otherwise each packet can be larger than specified</param>
        public static IObservable<byte[]> BufferBytes(this IObservable<byte[]> stream, int bytesPerPacket, bool isStrict)
        {
            if (isStrict)
            {
                return Observable.Create<byte[]>(obs =>
                {
                    var buffer = new byte[bytesPerPacket];
                    int position = 0;

                    return stream.Subscribe((b) =>
                    {
                        var count = b.Length;
                        var offset = 0;

                        while (count > 0)
                        {
                            var dataToRead = buffer.Length - position;
                            if (dataToRead > count)
                            {
                                dataToRead = count;
                            }

                            Buffer.BlockCopy(b, offset, buffer, position, dataToRead);

                            count -= dataToRead;
                            offset += dataToRead;
                            position += dataToRead;

                            if (position >= buffer.Length)
                            {
                                obs.OnNext(buffer);
                                buffer = new byte[bytesPerPacket];
                                position = 0;
                            }
                        }
                    },
                    obs.OnError,
                    () =>
                    {
                        if (position > 0)
                        {
                            // buffer will never be completely full at this point
                            // always have to copy it over!
                            var lastBytes = new byte[position];
                            Buffer.BlockCopy(buffer, 0, lastBytes, 0, position);
                            obs.OnNext(lastBytes);
                        }
                        obs.OnCompleted();
                    });
                });
            }
            else
            {
                return Observable.Create<byte[]>(obs =>
                {
                    long currentCount = 0;
                    var bufferList = new Queue<byte[]>();

                    return stream.Subscribe((b) =>
                    {
                        if (b.Length == 0) { return; }

                        if(b.Length + currentCount >= bytesPerPacket)
                        {
                            if (currentCount == 0)
                            {
                                obs.OnNext(b);
                            }
                            else
                            {
                                var bytes = new byte[b.Length + currentCount];
                                var offset = 0;
                                while (bufferList.Count > 0)
                                {
                                    var data = bufferList.Dequeue();
                                    Buffer.BlockCopy(data, 0, bytes, offset, data.Length);
                                    offset += data.Length;
                                }
                                Buffer.BlockCopy(b, 0, bytes, offset, b.Length);
                                bufferList.Clear();
                                currentCount = 0;
                                obs.OnNext(bytes);
                            }
                        }
                        else
                        {
                            bufferList.Enqueue(b);
                            currentCount += b.Length;
                        }
                    },
                    obs.OnError,
                    () =>
                    {
                        if(bufferList.Count > 0)
                        {
                            var bytes = new byte[currentCount];
                            var offset = 0;
                            while (bufferList.Count > 0)
                            {
                                var data = bufferList.Dequeue();
                                Buffer.BlockCopy(data, 0, bytes, offset, data.Length);
                                offset += data.Length;
                            }
                            obs.OnNext(bytes);
                        }

                        obs.OnCompleted();
                    });
                });
            }
        }

        /// <summary>
        /// Buffersize is what will be passed to the 'Read' on the stream, as such, the chunks will be bufferSize
        /// or smaller depending on the implementation of the underlying stream.
        /// NOTE: I recommend wrapping the read stream with a 'BufferedStream' if appropriate
        /// </summary>
        /// <param name="readStream">Stream you want to turn into an IObservable</param>
        /// <param name="bufferSize">Max size of the chunks</param>
        public static IObservable<byte[]> ToObservable(this Stream readStream, int bufferSize)
        {
            return ReadStream(readStream, bufferSize).ToObservable(Scheduler.Default);
        }

        public static async Task UseWriteStream(this IObserver<byte[]> observer, Func<Stream, Task> writeStreamScope, Action firstHit = null)
        {
            try
            {
                using (var stream = new ObserverWriteStream(observer, firstHit))
                {
                    await writeStreamScope(stream).ConfigureAwait(false);
                    observer.OnCompleted();
                }
            }
            catch(Exception e)
            {
                observer.OnError(e);
            }
        }

        private static IEnumerable<byte[]> ReadStream(Stream stream, int bufferSize)
        {
            using (stream)
            {
                var buffer = new byte[bufferSize];
                int bytesRead = stream.Read(buffer, 0, bufferSize);
                while (bytesRead > 0)
                {
                    if (bytesRead == buffer.Length)
                    {
                        yield return buffer;
                        buffer = new byte[bufferSize];
                    }
                    else
                    {
                        var data = new byte[bytesRead];
                        Buffer.BlockCopy(buffer, 0, data, 0, bytesRead);
                        yield return data;
                    }

                    bytesRead = stream.Read(buffer, 0, bufferSize);
                }
            }
        }
    }
}
