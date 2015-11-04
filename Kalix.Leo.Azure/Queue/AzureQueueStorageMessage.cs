using Kalix.Leo.Queue;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public sealed class AzureQueueStorageMessage : IQueueMessage
    {
        private readonly CloudQueue _queue;
        private readonly CloudQueueMessage _message;
        private readonly Lazy<string> _strMessage;
        private readonly IDisposable _lockWatcher;

        private bool _isDisposed;

        public AzureQueueStorageMessage(CloudQueue queue, CloudQueueMessage message, TimeSpan maxMessageTime)
        {
            _message = message;
            _queue = queue;
            _strMessage = new Lazy<string>(() => _message.AsString);

            var waitFor = ((message.NextVisibleTime.HasValue ? DateTime.UtcNow.AddMinutes(1) : message.NextVisibleTime.Value.UtcDateTime) - DateTime.UtcNow).Subtract(TimeSpan.FromSeconds(10));

            _lockWatcher = AsyncEnumerableEx.CreateTimer(waitFor)
                .Select(i => _queue.UpdateMessageAsync(_message, TimeSpan.FromMinutes(1), MessageUpdateFields.Visibility))
                .Unwrap()
                .TakeUntilDisposed(maxMessageTime, t => _isDisposed = true);
        }

        public string Message
        {
            get 
            { 
                if(_isDisposed)
                {
                    throw new ObjectDisposedException("AzureQueueStorageMessage");
                }

                return _strMessage.Value; 
            }
        }

        public async Task Complete()
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException("AzureQueueStorageMessage");
            }

            await _queue.DeleteMessageAsync(_message).ConfigureAwait(false);
        }

        public void Dispose()
        {
            _isDisposed = true;
            _lockWatcher.Dispose();
        }
    }
}
