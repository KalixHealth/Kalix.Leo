using Kalix.Leo.Queue;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public sealed class AzureQueueStorageMessage : IQueueMessage
    {
        private readonly CloudQueue _queue;
        private readonly CloudQueueMessage _message;
        private readonly Lazy<string> _strMessage;

        public AzureQueueStorageMessage(CloudQueue queue, CloudQueueMessage message)
        {
            _message = message;
            _queue = queue;
            _strMessage = new Lazy<string>(() => _message.AsString);
        }

        public DateTimeOffset? NextVisible => _message.NextVisibleTime;
        public string Message => _strMessage.Value;

        public Task ExtendVisibility(TimeSpan span)
        {
            return _queue.UpdateMessageAsync(_message, span, MessageUpdateFields.Visibility);
        }

        public async Task Complete()
        {
            await _queue.DeleteMessageAsync(_message).ConfigureAwait(false);
        }
    }
}
