using Kalix.Leo.Queue;
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
        private readonly CloudQueue _queue;
        private readonly TimeSpan _queueMessageTimeout;

        public AzureQueueStorage(CloudQueueClient queueClient, string queue, TimeSpan queueMessageTimeout)
        {
            _queue = queueClient.GetQueueReference(queue);
            _queueMessageTimeout = queueMessageTimeout;
        }

        public Task SendMessage(string data)
        {
            return _queue.AddMessageAsync(new CloudQueueMessage(data));
        }

        public async Task<IEnumerable<IQueueMessage>> ListenForNextMessage(int maxMessages, CancellationToken token)
        {
            var messages = await _queue.GetMessagesAsync(maxMessages, TimeSpan.FromMinutes(1), null, null).ConfigureAwait(false);
            return messages.Select(m => new AzureQueueStorageMessage(_queue, m, _queueMessageTimeout)).ToList();
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
