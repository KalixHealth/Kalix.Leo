using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Queue.Messaging
{
    public class AzureQueue : IQueue
    {
        private readonly string _queue;
        private readonly string _serviceBusConnectionString;
        private readonly MessagingFactory _factory;

        public AzureQueue(string serviceBusConnectionString, string queue)
        {
            _factory = MessagingFactory.CreateFromConnectionString(serviceBusConnectionString);
            _queue = queue;
            _serviceBusConnectionString = serviceBusConnectionString;
        }

        public AzureQueue(MessagingFactory factory, string queue)
        {
            _factory = factory;
            _queue = queue;
        }

        public Task SendMessage(string data)
        {
            var client = _factory.CreateQueueClient(_queue);
            var message = new BrokeredMessage(data);
            return client.SendAsync(message);
        }

        public IDisposable SetupMessageListener(Func<string, Task> executeMessage, Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null)
        {
            // By default use the number of processors
            var prefetchCount = messagesToProcessInParallel ?? Environment.ProcessorCount;
            var client = _factory.CreateQueueClient(_queue);

            var options = new OnMessageOptions
            {
                AutoComplete = true,
                MaxConcurrentCalls = prefetchCount
            };

            EventHandler<ExceptionReceivedEventArgs> handler = null;
            if (uncaughtException != null)
            {
                handler = new EventHandler<ExceptionReceivedEventArgs>((s, e) => uncaughtException(e.Exception));
                options.ExceptionReceived += handler;
            }

            client.OnMessageAsync((m) =>
            {
                var data = m.GetBody<string>();
                return executeMessage(data);
            }, options);

            return new SimpleDispose(() =>
            {
                client.Close();
                if(handler != null)
                {
                    options.ExceptionReceived -= handler;
                }
            });
        }

        public async Task CreateQueueIfExists()
        {
            var ns = NamespaceManager.CreateFromConnectionString(_serviceBusConnectionString);
            if (await ns.QueueExistsAsync(_queue))
            {
                var desc = new QueueDescription(_queue);
                desc.SupportOrdering = false; // should learn to make stuff indepotent :)
                desc.MaxSizeInMegabytes = 5120; // 5gb is the max
                await ns.CreateQueueAsync(desc);
            }
        }

        public async Task DeleteQueueIfExists()
        {
            var ns = NamespaceManager.CreateFromConnectionString(_serviceBusConnectionString);
            if (await ns.QueueExistsAsync(_queue))
            {
                await ns.DeleteQueueAsync(_queue);
            }
        }

        private sealed class SimpleDispose : IDisposable
        {
            private readonly Action _onDispose;
            private bool _isDisposed;

            public SimpleDispose(Action onDispose)
            {
                _onDispose = onDispose;
            }

            public void Dispose()
            {
                if (!_isDisposed)
                {
                    _onDispose();
                    _isDisposed = true;
                }
            }
        }
    }
}
