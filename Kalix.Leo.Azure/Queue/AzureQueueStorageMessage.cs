using Kalix.Leo.Queue;
using Microsoft.ServiceBus.Messaging;
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
        private readonly Action _onDisposed;
        private readonly IDisposable _lockWatcher;

        public AzureQueueStorageMessage(CloudQueue queue, CloudQueueMessage message, Action onDisposed)
        {
            _message = message;
            _queue = queue;
            _strMessage = new Lazy<string>(() => _message.AsString);
            _onDisposed = onDisposed;

            var waitFor = ((message.NextVisibleTime.HasValue ? DateTime.UtcNow.AddMinutes(1) : message.NextVisibleTime.Value.UtcDateTime) - DateTime.UtcNow).Subtract(TimeSpan.FromSeconds(10));
            _lockWatcher = Observable.Interval(waitFor)
                .Select(i => _queue.UpdateMessageAsync(_message, TimeSpan.FromMinutes(1), MessageUpdateFields.Visibility))
                .Subscribe();
        }

        public string Message
        {
            get { return _strMessage.Value; }
        }

        public Task Complete()
        {
            return _queue.DeleteMessageAsync(_message);
        }

        public void Dispose()
        {
            _lockWatcher.Dispose();
            _onDisposed();
        }
    }
}
