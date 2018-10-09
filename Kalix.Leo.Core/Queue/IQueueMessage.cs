using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Queue
{
    /// <summary>
    /// Queue message holds the data from a queue, but also is holding a lock on the message
    /// 
    /// If you have successfully handled a message make sure to complete it before disposing
    /// </summary>
    public interface IQueueMessage
    {
        // Gets when a message is next visible, important for long running tasks
        DateTimeOffset? NextVisible { get; }

        /// <summary>
        /// View the message data
        /// </summary>
        string Message { get; }

        /// <summary>
        /// Extends the time that the message should not be visible
        /// </summary>
        /// <param name="span">Amount of time to hide</param>
        Task ExtendVisibility(TimeSpan span);

        /// <summary>
        /// Indicate that the message has been successfully handled
        /// </summary>
        /// <returns>Task that completes when the message has been cleared from its originating queue</returns>
        Task Complete();
    }
}
