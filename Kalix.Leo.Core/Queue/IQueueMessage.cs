using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Queue
{
    /// <summary>
    /// Queue message holds the data from a queue, but also is holding a lock on the message
    /// 
    /// If you have successfully handled a message make sure to complete it before disposing
    /// </summary>
    public interface IQueueMessage : IDisposable
    {
        /// <summary>
        /// View the message data
        /// </summary>
        string Message { get; }

        /// <summary>
        /// Indicate that the message has been successfully handled
        /// </summary>
        /// <returns>Task that completes when the message has been cleared from its originating queue</returns>
        Task Complete();
    }
}
