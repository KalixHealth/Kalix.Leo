using Kalix.Leo.Queue;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public sealed class AzureQueueMessage : IQueueMessage
    {
        private readonly BrokeredMessage _message;
        private readonly Lazy<string> _strMessage;

        public AzureQueueMessage(BrokeredMessage message)
        {
            _message = message;
            _strMessage = new Lazy<string>(() => _message.GetBody<string>());
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
            _message.Dispose();
        }
    }
}
