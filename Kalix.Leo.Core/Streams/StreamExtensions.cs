using Kalix.Leo.Streams;
using System.Threading.Tasks;

namespace System.IO
{
    public static class StreamExtensions
    {
        public static Stream ToReadStream(this IObservable<byte> stream)
        {
            return new ObservableReadStream(stream);
        }

        public static async Task UseWriteStream(this IObserver<byte> observer, Func<Stream, Task> writeStreamScope, Action firstHit = null)
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
    }
}
