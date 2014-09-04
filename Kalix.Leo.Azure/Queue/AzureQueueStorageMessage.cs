using Kalix.Leo.Queue;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public sealed class AzureQueueStorageMessage : IQueueMessage
    {
        private readonly CloudQueue _queue;
        private readonly CloudQueueMessage _message;
        private readonly Lazy<string> _strMessage;
        private readonly Action<AzureQueueStorageMessage> _onDisposed;
        private readonly IDisposable _lockWatcher;

        private bool _isDisposed;

        public AzureQueueStorageMessage(CloudQueue queue, CloudQueueMessage message, Action<AzureQueueStorageMessage> onDisposed)
        {
            _message = message;
            _queue = queue;
            _strMessage = new Lazy<string>(() => _message.AsString);
            _onDisposed = onDisposed;

            var waitFor = ((message.NextVisibleTime.HasValue ? DateTime.UtcNow.AddMinutes(1) : message.NextVisibleTime.Value.UtcDateTime) - DateTime.UtcNow).Subtract(TimeSpan.FromSeconds(10));
            _lockWatcher = Observable.Interval(waitFor)
                .SelectMany(i => Observable.FromAsync(ct => _queue.UpdateMessageAsync(_message, TimeSpan.FromMinutes(1), MessageUpdateFields.Visibility, ct)))
                .Subscribe(u => { }, (e) => _isDisposed = true);
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
            _onDisposed(this);
        }
    }
}
