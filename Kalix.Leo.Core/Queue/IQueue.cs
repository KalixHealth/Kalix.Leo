using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Queue
{
    /// <summary>
    /// An interface to hide an underlying queue mechanism, if you can implement this interface you can use
    /// the Kalix engine
    /// </summary>
    public interface IQueue
    {
        /// <summary>
        /// Push a message into the queue
        /// </summary>
        /// <param name="data">The data to send</param>
        /// <param name="visibilityDelay">The time we want to wait before the queue is visible initially</param>
        /// <returns>A task that completes when the message has been queued</returns>
        Task SendMessage(string data, TimeSpan? visibilityDelay = null);

        /// <summary>
        /// Listen for messages originating from this queue
        /// </summary>
        /// <param name="maxMessages">Max number of messages to pull</param>
        /// <param name="visibility">The amount of time to make the messages invisible for</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>
        /// An list of queue messages - might be empty (task may take some time to return due to long polling of messages...)
        /// </returns>
        IAsyncEnumerable<IQueueMessage> ListenForMessages(int maxMessages, TimeSpan visibility, TimeSpan delayWhenEmpty, Action<Exception> uncaughtException = null, CancellationToken token = default);

        /// <summary>
        /// Make sure that the queue has been created
        /// </summary>
        /// <returns>Task that completes if/when the queue is created</returns>
        Task CreateQueueIfNotExists();

        /// <summary>
        /// Delete the queue if it exists
        /// </summary>
        /// <returns>Task that completes if/when the queue is deleted</returns>
        Task DeleteQueueIfExists();
    }
}
