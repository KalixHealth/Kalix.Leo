using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Queue.Messaging
{
    public class AzureQueueClient : IQueueClient
    {
        private readonly CloudQueueClient _queueClient;

        public AzureQueueClient(CloudQueueClient queueClient)
        {
            _queueClient = queueClient;
        }

        public Task CreateMessage(string queue, string message)
        {
            var q = _queueClient.GetQueueReference(queue);
            var m = new CloudQueueMessage(message);
            return q.AddMessageAsync(m);
        }

        public IDisposable SetupMessageListener(string queue, Func<string, Task> executeMessage)
        {
            var q = _queueClient.GetQueueReference(queue);
            return new MessageListener(q, executeMessage);
        }

        public Task CreateQueue(string queue)
        {
            var q = _queueClient.GetQueueReference(queue);
            return q.CreateIfNotExistsAsync();
        }

        public Task DeleteQueue(string queue)
        {
            var q = _queueClient.GetQueueReference(queue);
            return q.DeleteIfExistsAsync();
        }
    }
}
