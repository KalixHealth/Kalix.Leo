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
        private static readonly TimeSpan VisiblityTimeout = TimeSpan.FromMinutes(10);
        private static readonly TimeSpan VisibilityBuffer = TimeSpan.FromMinutes(2);

        private const int PendingMessagesFactor = 10;

        private readonly IQueue _indexQueue;
        private readonly IQueue _backupIndexQueue;
        private readonly Dictionary<string, Type> _typeIndexers;
        private readonly Dictionary<string, Type> _pathIndexers;
        private readonly Func<string, Type> _typeNameResolver;
        private readonly Func<Type, object> _typeResolver;

        public IndexListener(IQueue indexQueue, IQueue backupIndexQueue, Func<Type, object> typeResolver, Func<string, Type> typeNameResolver = null)
        {
            _indexQueue = indexQueue;
            _backupIndexQueue = backupIndexQueue;
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
                var messages = new Dictionary<string, List<IQueueMessage>>();
                var hash = new Dictionary<string, Tuple<Task, List<IQueueMessage>>>();

                // Special queue system
                // We grab messages as soon as we have free slots, and then queue them up by type and org
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        // Clean up any finished tasks
                        foreach (var item in hash.ToList())
                        {
                            var task = item.Value.Item1;
                            var hasError = task.IsCanceled || task.IsFaulted;
                            if (task.IsCompleted || hasError)
                            {
                                // We do the completions here so we don't get overlap with the visibility extensions
                                if (!hasError)
                                {
                                    try
                                    {
                                        // Don't bother trying to close anything that is already broken...
                                        await Task.WhenAll(item.Value.Item2.Where(m => !m.IsComplete).Select(m => m.Complete())).ConfigureAwait(false);
                                    }
                                    catch (Exception e)
                                    {
                                        uncaughtException?.Invoke(new Exception("Could not complete messages", e));
                                    }
                                }
                                hash.Remove(item.Key);
                            }
                        }

                        // Now that any hashes that are done are cleared up, extend any messages that need it...
                        var buffer = DateTimeOffset.Now.Add(VisibilityBuffer);
                        var extendTasks = hash.Values
                            .SelectMany(v => v.Item2)
                            .Concat(messages.Values.SelectMany(q => q))
                            // Don't bother extending the already broken ones...
                            .Where(m => !m.IsComplete && (!m.NextVisible.HasValue || m.NextVisible < buffer))
                            .Select(async m =>
                            {
                                try
                                {
                                    await m.ExtendVisibility(VisiblityTimeout).ConfigureAwait(false);
                                }
                                catch (Exception e)
                                {
                                    uncaughtException?.Invoke(new Exception("Could not extend messages", e));
                                }
                            });

                        await Task.WhenAll(extendTasks).ConfigureAwait(false);

                        // Wait until we have free slots...
                        if (hash.Count >= maxMessages)
                        {
                            await Task.Delay(2000).ConfigureAwait(false);
                            continue;
                        }

                        if (messages.Any())
                        {
                            foreach (var m in messages.ToList())
                            {
                                // We might have lost the message already
                                messages[m.Key] = m.Value.Where(me => !me.IsComplete).ToList();
                                if (!messages[m.Key].Any())
                                {
                                    messages.Remove(m.Key);
                                    continue;
                                }

                                if (!hash.ContainsKey(m.Key))
                                {
                                    hash[m.Key] = Tuple.Create(ExecuteMessages(m.Value, uncaughtException), m.Value);
                                    messages.Remove(m.Key);
                                }
                            }
                            
                            if ((messages.Count + hash.Count) >= maxMessages)
                            {
                                await Task.Delay(2000).ConfigureAwait(false);
                                continue;
                            }
                        }

                        // Get more messages, pre-build the collection so we can collect as much as possible in one go
                        bool shouldDelay = false;
                        var totalPending = 0;

                        do
                        {
                            var nextSet = await _indexQueue.ListenForNextMessage(maxMessages, VisiblityTimeout, ct).ConfigureAwait(false);
                            totalPending += nextSet.Count();
                            if (!nextSet.Any())
                            {
                                if (_backupIndexQueue != null)
                                {
                                    nextSet = await _backupIndexQueue.ListenForNextMessage(maxMessages, VisiblityTimeout, ct).ConfigureAwait(false);
                                    totalPending += nextSet.Count();
                                }

                                if (!nextSet.Any())
                                {
                                    shouldDelay = true;
                                    break;
                                }
                            }

                            foreach (var g in nextSet.GroupBy(FindKey))
                            {
                                if (!messages.ContainsKey(g.Key))
                                {
                                    messages[g.Key] = new List<IQueueMessage>();
                                }
                                messages[g.Key].AddRange(g);
                            }
                        }
                        while (messages.Count < maxMessages && totalPending < maxMessages * PendingMessagesFactor);

                        if (shouldDelay && !messages.Any())
                        {
                            await Task.Delay(2000, ct).ConfigureAwait(false);
                        }
                        
                    }
                    catch(Exception e)
                    {
                        if(uncaughtException != null)
                        {
                            var ex = new Exception("An exception occured in the message handling loop: " + e.Message, e);
                            uncaughtException(ex);
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
                var baseDetails = messages
                    .Select(m => JsonConvert.DeserializeObject<StoreDataDetails>(m.Message))
                    .GroupBy(m => m.Metadata.ContainsKey(MetadataConstants.ReindexMetadataKey) && m.Metadata[MetadataConstants.ReindexMetadataKey] == "true")
                    .ToList();

                foreach (var g in baseDetails)
                {
                    var isReindex = g.Key;
                    var details = g.ToList();

                    // Make sure to remove the reindex key, we don't want it to propagate
                    if (isReindex)
                    {
                        foreach(var d in details)
                        {
                            d.Metadata.Remove(MetadataConstants.ReindexMetadataKey);
                        }
                    }

                    bool hasData = false;
                    string type = null;
                    if (details[0].Metadata.ContainsKey(MetadataConstants.TypeMetadataKey))
                    {
                        type = details[0].Metadata[MetadataConstants.TypeMetadataKey];
                        if (_typeIndexers.ContainsKey(type))
                        {
                            var indexer = (IIndexer)_typeResolver(_typeIndexers[type]);

                            // Do need to reindex the same id multiple times
                            details = details.GroupBy(d => d.Id.Value).Select(d => d.First()).ToList();
                            if (isReindex && indexer is IReindexer)
                            {
                                await (indexer as IReindexer).Reindex(details).ConfigureAwait(false);
                            }
                            else
                            {
                                await indexer.Index(details).ConfigureAwait(false);
                            }
                            hasData = true;
                        }
                    }

                    if (!hasData)
                    {
                        var key = _pathIndexers.Keys.Where(k => details[0].BasePath.StartsWith(k)).FirstOrDefault();
                        if (key != null)
                        {
                            var indexer = (IIndexer)_typeResolver(_pathIndexers[key]);

                            // Only need to index the same path once
                            details = details.GroupBy(d => d.BasePath).Select(d => d.First()).ToList();
                            if (isReindex && indexer is IReindexer)
                            {
                                await (indexer as IReindexer).Reindex(details).ConfigureAwait(false);
                            }
                            else
                            {
                                await indexer.Index(details).ConfigureAwait(false);
                            }
                            hasData = true;
                        }
                    }

                    if (!hasData)
                    {
                        throw new InvalidOperationException("Could not find indexer for record: container=" + details[0].Container + ", path=" + details[0].BasePath + ", type=" + (type ?? "None") + ":" + details.Count);
                    }
                }
            }
            catch(Exception e)
            {
                if(uncaughtException != null)
                {
                    var ex = new Exception("An exception occurred while handling a message: " + e.Message, e);
                    uncaughtException(ex);
                }
                throw;
            }
        }
    }
}
