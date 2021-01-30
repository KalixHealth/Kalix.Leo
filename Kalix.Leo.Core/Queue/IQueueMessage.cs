using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Queue
{
    /// <summary>
    /// Queue message holds the data from a queue, but also is holding a lock on the message
    /// and will continue to refresh the lock until it is disposed
    /// 
    /// If you have successfully handled a message make sure to complete it before disposing
    /// </summary>
    public interface IQueueMessage : IAsyncDisposable
    {
        /// <summary>
        /// View the message data
        /// </summary>
        string Message { get; }

        /// <summary>
        /// If the message threw an error while extending, or if complete was called, this will be true
        /// </summary>
        bool IsComplete { get; }

        /// <summary>
        /// Indicate that the message has been successfully handled
        /// </summary>
        /// <returns>Task that completes when the message has been cleared from its originating queue</returns>
        Task<bool> Complete();
    }
}
