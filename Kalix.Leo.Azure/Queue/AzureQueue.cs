using Kalix.Leo.Queue;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
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

        public IObservable<IQueueMessage> ListenForMessages(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null)
        {
            return Observable.Create<IQueueMessage>(observer =>
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

                client.OnMessage((m) =>
                {
                    var message = new AzureQueueMessage(m);
                    observer.OnNext(message);
                }, options);

                // Return the method to call on dispose
                return () =>
                {
                    client.Close();
                    if (handler != null)
                    {
                        options.ExceptionReceived -= handler;
                    }
                };
            });
        }

        public async Task CreateQueueIfNotExists()
        {
            var ns = NamespaceManager.CreateFromConnectionString(_serviceBusConnectionString);
            if (!await ns.QueueExistsAsync(_queue))
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
    }
}
