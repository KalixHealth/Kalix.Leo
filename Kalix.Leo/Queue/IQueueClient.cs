using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Queue
{
    /// <summary>
    /// An interface to hide an underlying queue mechanism, if you can implement this interface you can use
    /// the Kalix engine
    /// </summary>
    public interface IQueueClient
    {
        Task CreateMessage(string queue, string message);

        IDisposable SetupMessageListener(string queue, Func<string, Task> executeMessage);

        Task CreateQueue(string queue);
        Task DeleteQueue(string queue);

        //Task ReadMessage(string queue, Func<string, Task> executeMessage);
        //Task ReadMessages(string queue, Func<IEnumerable<string>, Task> executeMessages, int numberOfMessages = 32);
    }
}
