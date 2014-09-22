using Kalix.Leo.Indexing;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Listeners
{
    public class IndexListener : IIndexListener
    {
        private readonly IQueue _indexQueue;
        private readonly Dictionary<string, Type> _typeIndexers;
        private readonly Dictionary<string, Type> _pathIndexers;
        private readonly Func<string, Type> _typeNameResolver;
        private readonly Func<Type, object> _typeResolver;

        public IndexListener(IQueue indexQueue, Func<Type, object> typeResolver, Func<string, Type> typeNameResolver = null)
        {
            _indexQueue = indexQueue;
            _typeIndexers = new Dictionary<string, Type>();
            _pathIndexers = new Dictionary<string, Type>();
            _typeNameResolver = typeNameResolver ?? (s => Type.GetType(s, false));
            _typeResolver = typeResolver;
        }

        public void RegisterPathIndexer(string basePath, Type indexer)
        {
            if(!typeof(IIndexer).GetTypeInfo().IsAssignableFrom(indexer.GetTypeInfo()))
            {
                throw new ArgumentException("The type specified to register as an indexer does not implement IIndexer", "indexer");
            }

            if (_pathIndexers.ContainsKey(basePath))
            {
                throw new InvalidOperationException("Already have a indexer for base path: " + basePath);
            }

            _pathIndexers[basePath] = indexer;
        }

        public void RegisterTypeIndexer<T>(Type indexer)
        {
            RegisterTypeIndexer(typeof(T), indexer);
        }

        public void RegisterTypeIndexer(Type type, Type indexer)
        {
            if (!typeof(IIndexer).GetTypeInfo().IsAssignableFrom(indexer.GetTypeInfo()))
            {
                throw new ArgumentException("The type specified to register as an indexer does not implement IIndexer", "indexer");
            }

            if (_typeIndexers.ContainsKey(type.FullName))
            {
                throw new InvalidOperationException("Already have a indexer for type: " + type);
            }

            _typeIndexers[type.FullName] = indexer;
        }

        public IDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null)
        {
            var maxMessages = messagesToProcessInParallel ?? Environment.ProcessorCount;
            var token = new CancellationTokenSource();
            var ct = token.Token;

            Task.Run(async () =>
            {
                // Special queue system
                // We grab messages as soon as we have free slots, and then queue them up by type and org
                var hash = new Dictionary<string, Task>();
                while(!ct.IsCancellationRequested)
                {
                    try
                    {
                        // Clean up any finished tasks
                        foreach (var item in hash.ToList())
                        {
                            if (item.Value.IsCanceled || item.Value.IsCompleted || item.Value.IsFaulted)
                            {
                                try
                                {
                                    hash.Remove(item.Key);
                                }
                                catch (Exception e)
                                {
                                    if (uncaughtException != null)
                                    {
                                        uncaughtException(e);
                                    }
                                }
                            }
                        }

                        // Wait until we have free slots...
                        if (hash.Count >= maxMessages)
                        {
                            await Task.Delay(1000, ct).ConfigureAwait(false);
                            continue;
                        }

                        // Get more messages
                        var messages = await _indexQueue.ListenForNextMessage(maxMessages, ct).ConfigureAwait(false);
                        if (!messages.Any())
                        {
                            await Task.Delay(2000, ct).ConfigureAwait(false);
                            continue;
                        }

                        // Group the messages into buckets
                        foreach (var g in messages.GroupBy(FindKey))
                        {
                            var items = g.ToList();
                            if (hash.ContainsKey(g.Key))
                            {
                                // If the bucket is already running, queue the next action
                                hash[g.Key] = hash[g.Key]
                                    .ContinueWith(t => ExecuteMessages(items, uncaughtException), ct)
                                    .Unwrap();
                            }
                            else
                            {
                                // Start a new independant thread
                                hash[g.Key] = Task.Run(() => ExecuteMessages(items, uncaughtException), ct);
                            }
                        }
                    }
                    catch(Exception e)
                    {
                        if(uncaughtException != null)
                        {
                            uncaughtException(e);
                        }
                    }
                }
            }, ct);

            return token;
        }

        private string FindKey(IQueueMessage message)
        {
            var details = JsonConvert.DeserializeObject<StoreDataDetails>(message.Message);
            var firstPath = details.BasePath.Split(new char[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries)[0];
            return details.Container + "_" + firstPath;
        }

        private async Task ExecuteMessages(IEnumerable<IQueueMessage> messages, Action<Exception> uncaughtException)
        {
            try
            {
                var details = messages.Select(m => JsonConvert.DeserializeObject<StoreDataDetails>(m.Message)).ToList();

                bool hasData = false;
                if(details[0].Metadata.ContainsKey(MetadataConstants.TypeMetadataKey))
                {
                    var type = details[0].Metadata[MetadataConstants.TypeMetadataKey];
                    if(_typeIndexers.ContainsKey(type))
                    {
                        var indexer = (IIndexer)_typeResolver(_typeIndexers[type]);
                        await indexer.Index(details).ConfigureAwait(false);
                        hasData = true;
                    }
                }
                
                if(!hasData)
                {
                    var key = _pathIndexers.Keys.Where(k => details[0].BasePath.StartsWith(k)).FirstOrDefault();
                    if (key != null)
                    {
                        var indexer = (IIndexer)_typeResolver(_pathIndexers[key]);
                        await indexer.Index(details).ConfigureAwait(false);
                        hasData = true;
                    }
                }

                if(!hasData)
                {
                    throw new InvalidOperationException("Could not find indexer for record: container=" + details[0].Container + ", path=" + details[0].BasePath + ":\r\n" + details.Count);
                }

                await Task.WhenAll(messages.Select(m => m.Complete())).ConfigureAwait(false);
            }
            catch(Exception e)
            {
                if(uncaughtException != null)
                {
                    uncaughtException(e);
                }
                throw;
            }
            finally
            {
                foreach (var m in messages)
                {
                    m.Dispose();
                }
            }
        }
    }
}
