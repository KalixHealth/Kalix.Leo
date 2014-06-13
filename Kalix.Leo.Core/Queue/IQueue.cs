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
        /// <summary>
        /// Push a message into the queue
        /// </summary>
        /// <param name="data">The data to send</param>
        /// <returns>A task that completes when the message has been queued</returns>
        Task SendMessage(string data);

        /// <summary>
        /// Listen for messages originating from this queue
        /// </summary>
        /// <param name="uncaughtException">If there are any internal errors you can listen to them here</param>
        /// <param name="messagesToProcessInParallel">The number of messages to read at once</param>
        /// <param name="millisecondPollInterval">The amount of time to poll in milliseconds</param>
        /// <returns>
        /// An observable of queue messages
        /// 
        /// Note: the queue messages must be completed and disposed, otherwise they will return to the queue
        /// </returns>
        IObservable<IQueueMessage> ListenForMessages(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null, int millisecondPollInterval = 2000);

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
