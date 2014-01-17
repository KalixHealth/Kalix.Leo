using Kalix.Leo.Streams;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace System.IO
{
    public static class StreamExtensions
    {
        public static IObservable<byte[]> BufferBytes(this IObservable<byte[]> stream, int bytesPerPacket)
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
                (e) => { obs.OnError(e); },
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
                    await writeStreamScope(stream);
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
                    var data = new byte[bytesRead];
                    Buffer.BlockCopy(buffer, 0, data, 0, bytesRead);
                    yield return data;

                    bytesRead = stream.Read(buffer, 0, bufferSize);
                }
            }
        }
    }
}
