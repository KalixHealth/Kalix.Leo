using Kalix.Leo.Queue;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public sealed class AzureQueueMessage : IQueueMessage
    {
        private readonly BrokeredMessage _message;
        private readonly Lazy<string> _strMessage;
        private readonly Action _onDisposed;
        private readonly IDisposable _lockWatcher;

        public AzureQueueMessage(BrokeredMessage message, Action onDisposed)
        {
            _message = message;
            _strMessage = new Lazy<string>(() => _message.GetBody<string>());
            _onDisposed = onDisposed;

            var waitFor = (message.LockedUntilUtc - DateTime.UtcNow).Subtract(TimeSpan.FromSeconds(10));
            _lockWatcher = Observable.Interval(waitFor)
                .Select(i => _message.RenewLockAsync())
                .Subscribe();
        }

        public string Message
        {
            get { return _strMessage.Value; }
        }

        public Task Complete()
        {
            return _message.CompleteAsync();
        }

        public void Dispose()
        {
            _lockWatcher.Dispose();
            _message.Dispose();
            _onDisposed();
        }
    }
}
