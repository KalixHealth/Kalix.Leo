using Kalix.Leo.Queue;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public sealed class AzureQueueStorageMessage : IQueueMessage
    {
        private static readonly TimeSpan MaxVisibilityTimeout = TimeSpan.FromDays(7);

        private readonly CloudQueue _queue;
        private readonly CloudQueueMessage _message;
        private readonly Lazy<string> _strMessage;

        public AzureQueueStorageMessage(CloudQueue queue, CloudQueueMessage message)
        {
            _message = message;
            _queue = queue;
            _strMessage = new Lazy<string>(() => _message.AsString);
            IsComplete = false;
        }

        public DateTimeOffset? NextVisible => _message.NextVisibleTime;
        public string Message => _strMessage.Value;
        public bool IsComplete { get; private set; }

        public async Task<bool> ExtendVisibility(TimeSpan span)
        {
            if (span > MaxVisibilityTimeout) { span = MaxVisibilityTimeout; }

            try
            {
                await _queue.UpdateMessageAsync(_message, span, MessageUpdateFields.Visibility).ConfigureAwait(false);
                return true;
            }
            catch (StorageException e)
            {
                IsComplete = true;
                if (e.RequestInformation.HttpStatusCode == 404) { return false; }
                throw e.Wrap(_queue.Name);
            }
        }

        public async Task<bool> Complete()
        {
            IsComplete = true;
            try
            {
                await _queue.DeleteMessageAsync(_message).ConfigureAwait(false);
                return true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404) { return false; }
                throw e.Wrap(_queue.Name);
            }
        }
    }
}
