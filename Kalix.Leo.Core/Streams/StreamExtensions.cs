using Kalix.Leo.Streams;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Linq;

namespace System.IO
{
    public static class StreamExtensions
    {
        public static IObservable<byte[]> BufferBytes(this IObservable<byte[]> stream, int bytesPerPacket)
        {
            return stream
                .SelectMany(b => b)
                .Buffer(bytesPerPacket)
                .Select(b => b.ToArray());
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
