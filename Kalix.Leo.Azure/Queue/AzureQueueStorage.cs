using Kalix.Leo.Queue;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public class AzureQueueStorage : IQueue
    {
        private static readonly TimeSpan MaxVisibilityTimeout = TimeSpan.FromDays(7);
        private const int MaxPollingAmountAllowed = 32;

        private readonly CloudQueue _queue;

        public AzureQueueStorage(CloudQueueClient queueClient, string queue)
        {
            _queue = queueClient.GetQueueReference(queue);
        }

        public Task SendMessage(string data, TimeSpan? visibilityDelay = null)
        {
            return Execute(q => q.AddMessageAsync(new CloudQueueMessage(data), null, visibilityDelay, null, null));
        }

        public async Task<IEnumerable<IQueueMessage>> ListenForNextMessage(int maxMessages, TimeSpan visibility, CancellationToken token)
        {
            if (visibility > MaxVisibilityTimeout) { visibility = MaxVisibilityTimeout; }
            if (maxMessages > MaxPollingAmountAllowed) { maxMessages = MaxPollingAmountAllowed; }

            try
            {
                var messages = await _queue.GetMessagesAsync(maxMessages, visibility, null, null).ConfigureAwait(false);
                return messages.Select(m => new AzureQueueStorageMessage(_queue, m)).ToList();
            }
            catch (StorageException e)
            {
                throw e.Wrap("Queue: " + _queue.Name);
            }
        }

        public Task CreateQueueIfNotExists()
        {
            return Execute(q => q.CreateIfNotExistsAsync());
        }

        public Task DeleteQueueIfExists()
        {
            return Execute(q => q.DeleteIfExistsAsync());
        }

        private async Task Execute(Func<CloudQueue, Task> method)
        {
            try
            {
                await method(_queue).ConfigureAwait(false);
            }
            catch (StorageException e)
            {
                throw e.Wrap("Queue: " + _queue.Name);
            }
        }
    }
}
