using Kalix.Leo.Indexing;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Kalix.Leo.Listeners
{
    public class IndexListener : IIndexListener
    {
        private static readonly TimeSpan VisiblityTimeout = TimeSpan.FromMinutes(10);
        private static readonly TimeSpan DelayEmptyTimeout = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan TimeToWait = TimeSpan.FromSeconds(2);

        private readonly IQueue _indexQueue;
        private readonly IQueue _backupIndexQueue;
        private readonly Dictionary<string, Type> _typeIndexers;
        private readonly Dictionary<string, Type> _pathIndexers;
        private readonly Func<Type, object> _typeResolver;

        public IndexListener(IQueue indexQueue, IQueue backupIndexQueue, Func<Type, object> typeResolver)
        {
            _indexQueue = indexQueue;
            _backupIndexQueue = backupIndexQueue;
            _typeIndexers = new Dictionary<string, Type>();
            _pathIndexers = new Dictionary<string, Type>();
            _typeResolver = typeResolver;
        }

        public void RegisterPathIndexer(string basePath, Type indexer)
        {
            if(!typeof(IIndexer).GetTypeInfo().IsAssignableFrom(indexer.GetTypeInfo()))
            {
                throw new ArgumentException("The type specified to register as an indexer does not implement IIndexer", nameof(indexer));
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
                throw new ArgumentException("The type specified to register as an indexer does not implement IIndexer", nameof(indexer));
            }

            if (_typeIndexers.ContainsKey(type.FullName))
            {
                throw new InvalidOperationException("Already have a indexer for type: " + type);
            }

            _typeIndexers[type.FullName] = indexer;
        }

        private class GroupedMessages
        {
            public string Key { get; set; }
            public List<IQueueMessage> Messages { get; set; }
        }

        public IAsyncDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null)
        {
            var maxMessages = messagesToProcessInParallel ?? Environment.ProcessorCount;
            var token = new CancellationTokenSource();
            var ct = token.Token;

            var messages = _indexQueue.ListenForMessages(maxMessages, VisiblityTimeout, DelayEmptyTimeout, uncaughtException, ct);
            if (_backupIndexQueue != null)
            {
                // Combine messages where we take precendence on the primary messages
                var backupMessages = _backupIndexQueue.ListenForMessages(maxMessages, VisiblityTimeout, DelayEmptyTimeout, uncaughtException, ct);
                messages = new[] { messages, backupMessages }.Combine(AsyncEnumberableCombineType.FirstHasPriority, maxMessages, ct);
            }

            var actionBlock = new ActionBlock<IGrouping<string, IQueueMessage>>(async g =>
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    await ExecuteMessages(g);
                    await Task.WhenAll(g.Where(m => !m.IsComplete).Select(m => m.Complete()));
                }
                catch (Exception e)
                {
                    uncaughtException?.Invoke(e);
                }
                finally
                {
                    await Task.WhenAll(g.Select(m => m.DisposeAsync().AsTask()));
                }
            }, new ExecutionDataflowBlockOptions { BoundedCapacity = maxMessages, MaxDegreeOfParallelism = maxMessages });

            // Thread to push messages onto the action block to process in parallel
            var task = Task.Run(() =>
                messages.TimedBuffer(maxMessages, TimeToWait, ct)
                    .ForEachAwaitAsync(ma => ma.GroupBy(FindKey).ToAsyncEnumerable().ForEachAwaitAsync(g => actionBlock.SendAsync(g, ct), ct), ct)
            , ct);

            return AsyncDisposable.Create(async () =>
            {
                token.Cancel();
                actionBlock.Complete();
                try { await task; } catch { }
                try { await actionBlock.Completion; } catch { }
                token.Dispose();
            });
        }

        private string FindKey(IQueueMessage message)
        {
            var details = JsonConvert.DeserializeObject<StoreDataDetails>(message.Message);
            var firstPath = details.BasePath.Split(new char[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries)[0];
            return details.Container + "_" + firstPath;
        }

        private async Task ExecuteMessages(IEnumerable<IQueueMessage> messages)
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
                                await (indexer as IReindexer).Reindex(details);
                            }
                            else
                            {
                                await indexer.Index(details);
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
                                await (indexer as IReindexer).Reindex(details);
                            }
                            else
                            {
                                await indexer.Index(details);
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
                throw new Exception("An exception occurred while handling a message: " + e.Message, e);
            }
        }
    }
}
