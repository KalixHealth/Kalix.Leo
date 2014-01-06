using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Queue
{
    /// <summary>
    /// An interface to hide an underlying queue mechanism, if you can implement this interface you can use
    /// the Kalix engine
    /// </summary>
    public interface IQueue
    {
        Task SendMessage(string data);

        IDisposable SetupMessageListener(Func<string, Task> executeMessage, Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null);

        Task CreateQueueIfExists();
        Task DeleteQueueIfExists();
    }
}
