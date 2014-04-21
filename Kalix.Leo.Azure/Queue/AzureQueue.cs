using Kalix.Leo.Queue;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
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
                var cancel = new CancellationDisposable();

                Task.Run(async () =>
                {
                    int counter = 0;
                    object counterLock = new object();
                    while(!cancel.Token.IsCancellationRequested)
                    {
                        try
                        {
                            if (counter == prefetchCount)
                            {
                                await Task.Delay(1000).ConfigureAwait(false);
                            }
                            else
                            {
                                var messages = await client.ReceiveBatchAsync(prefetchCount - counter);

                                lock (counterLock)
                                {
                                    counter += messages.Count();
                                }

                                foreach (var m in messages)
                                {
                                    var message = new AzureQueueMessage(m, () => { Interlocked.Decrement(ref counter); });
                                    observer.OnNext(message);
                                }
                            }
                        }
                        catch(Exception e)
                        {
                            if(uncaughtException != null)
                            {
                                uncaughtException(e);
                            }
                            counter = 0;
                        }
                    }
                }, cancel.Token);

                // Return the method to call on dispose
                return () =>
                {
                    cancel.Dispose();
                    client.Close();
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
                desc.MaxDeliveryCount = 2000;
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
