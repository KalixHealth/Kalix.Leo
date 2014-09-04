using Kalix.Leo.Queue;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public class AzureQueueStorage : IQueue
    {
        private readonly CloudQueue _queue;
        private readonly TimeSpan _queueMessageMinTimeout;

        public AzureQueueStorage(CloudQueueClient queueClient, string queue, TimeSpan queueMessageMinTimeout)
        {
            _queue = queueClient.GetQueueReference(queue);
            _queueMessageMinTimeout = queueMessageMinTimeout;
        }

        public Task SendMessage(string data)
        {
            return _queue.AddMessageAsync(new CloudQueueMessage(data));
        }

        public IObservable<IQueueMessage> ListenForMessages(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null, int millisecondPollInterval = 2000)
        {
            var prefetchCount = messagesToProcessInParallel ?? Environment.ProcessorCount;
            object counterLock = new object();
            var waitingMessages = new List<AzureQueueStorageMessage>();

            // Wacky little function to throttle messages in parrallel
            // Allows new messages to be grabbed even when there are others waiting to be processed
            Func<Task<IEnumerable<AzureQueueStorageMessage>>> getNextMessages = async () =>
            {
                if (waitingMessages.Count <= prefetchCount)
                {
                    LeoTrace.WriteLine("Getting new messages in queue");
                    var messages = await _queue.GetMessagesAsync(prefetchCount, TimeSpan.FromMinutes(1), null, null).ConfigureAwait(false);

                    LeoTrace.WriteLine(string.Format("Found {0} messages to process, also waiting on {1} messages", messages.Count(), waitingMessages.Count));

                    lock (counterLock)
                    {
                        var toAdd = messages.Select(m => new AzureQueueStorageMessage(_queue, m, (mm) =>
                        {
                            lock (counterLock)
                            {
                                waitingMessages.Remove(mm);
                            }
                        }));

                        waitingMessages.AddRange(toAdd);
                        return toAdd;
                    }
                }
                else
                {
                    LeoTrace.WriteLine("Waiting to poll in queue");
                    await Task.Delay(millisecondPollInterval).ConfigureAwait(false);
                    return new List<AzureQueueStorageMessage>();
                }
            };

            // The loop...
            return Observable.FromAsync(getNextMessages)
                .Catch<IEnumerable<AzureQueueStorageMessage>, Exception>((e) =>
                {
                    if(uncaughtException != null)
                    {
                        uncaughtException(e);
                    }
                    return Observable.Empty<IEnumerable<AzureQueueStorageMessage>>();
                })
                .SelectMany(m => m)
                .Repeat()
                .Timeout(_queueMessageMinTimeout)
                .Catch<AzureQueueStorageMessage, Exception>((e) =>
                {
                    if (uncaughtException != null && waitingMessages.Count > 0)
                    {
                        uncaughtException(new Exception("Timeout when waiting for messages, purging " + waitingMessages.Count + " messages", e));
                        
                        // Copy the list
                        var current = waitingMessages.ToList();
                        foreach (var c in current)
                        {
                            // The dispose will drop the messages
                            c.Dispose();
                        }
                    }
                    return Observable.Empty<AzureQueueStorageMessage>();
                })
                .Retry()
                .Repeat();
        }

        public Task CreateQueueIfNotExists()
        {
            return _queue.CreateIfNotExistsAsync();
        }

        public Task DeleteQueueIfExists()
        {
            return _queue.DeleteIfExistsAsync();
        }
    }
}
