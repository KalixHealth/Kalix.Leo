using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Kalix.Leo.Queue;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public class AzureQueueStorage : IQueue
    {
        private static readonly TimeSpan MaxVisibilityTimeout = TimeSpan.FromDays(7);
        private const int MaxPollingAmountAllowed = 32;

        private readonly QueueClient _queue;

        public AzureQueueStorage(QueueClient queue)
        {
            _queue = queue;
        }

        public Task SendMessage(string data, TimeSpan? visibilityDelay = null)
        {
            return _queue.SendMessageAsync(data, visibilityTimeout: visibilityDelay);
        }

        public async IAsyncEnumerable<IQueueMessage> ListenForMessages(int maxMessages, TimeSpan visibility, TimeSpan delayWhenEmpty, Action<Exception> uncaughtException = null, [EnumeratorCancellation] CancellationToken token = default)
        {
            if (visibility > MaxVisibilityTimeout) { visibility = MaxVisibilityTimeout; }
            if (maxMessages > MaxPollingAmountAllowed) { maxMessages = MaxPollingAmountAllowed; }

            while(!token.IsCancellationRequested)
            {
                QueueMessage[] messages;
                try
                {
                    messages = await _queue.ReceiveMessagesAsync(maxMessages, visibility, token);
                }
                catch (Exception e)
                {
                    uncaughtException?.Invoke(e);
                    await Task.Delay(delayWhenEmpty, token);
                    continue;
                }

                foreach (var m in messages)
                {
                    yield return new AzureQueueStorageMessage(_queue, m, visibility);
                }

                if (messages.Length == 0)
                {
                    await Task.Delay(delayWhenEmpty, token);
                }
            }
        }

        public Task CreateQueueIfNotExists()
        {
            return _queue.CreateIfNotExistsAsync();
        }

        public Task DeleteQueueIfExists()
        {
            return _queue.DeleteIfExistsAsync();
        }
    }
}
