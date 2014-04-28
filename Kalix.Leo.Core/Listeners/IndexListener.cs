using Kalix.Leo.Indexing;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
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

        private readonly LockFreeQueue<IQueueMessage> _messageQueue;
        private readonly ConcurrentDictionary<string, bool> _currentContainers;

        public IndexListener(IQueue indexQueue, Func<Type, object> typeResolver, Func<string, Type> typeNameResolver = null)
        {
            _indexQueue = indexQueue;
            _typeIndexers = new Dictionary<string, Type>();
            _pathIndexers = new Dictionary<string, Type>();
            _typeNameResolver = typeNameResolver ?? (s => Type.GetType(s, false));
            _typeResolver = typeResolver;

            _messageQueue = new LockFreeQueue<IQueueMessage>();
            _currentContainers = new ConcurrentDictionary<string, bool>();
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
            return _indexQueue.ListenForMessages(uncaughtException, messagesToProcessInParallel)
                .Subscribe(async (m) =>
                {
                    try
                    {
                        await MessageRecieved(m).ConfigureAwait(false);
                    }
                    catch(Exception e)
                    {
                        LeoTrace.WriteLine("Index error caught: " + e.Message);
                        if (uncaughtException != null) { uncaughtException(e); }
                    }
                }); // Start listening
        }

        private Task MessageRecieved(IQueueMessage message)
        {
            LeoTrace.WriteLine("Index message received");
            _messageQueue.Enqueue(message);
            return TryExecuteNext();
        }

        private async Task TryExecuteNext()
        {
            IQueueMessage nextMessage;
            if(_messageQueue.TryDequeue(out nextMessage))
            {
                var details = JsonConvert.DeserializeObject<StoreDataDetails>(nextMessage.Message);

                if(_currentContainers.TryAdd(details.Container, true))
                {
                    try
                    {
                        await ExecuteMessage(nextMessage).ConfigureAwait(false);
                        LeoTrace.WriteLine("Index message handled");
                    }
                    finally
                    {
                        bool temp;
                        _currentContainers.TryRemove(details.Container, out temp);
                    }

                    // We might have enqueued messages for this container...
                    await TryExecuteNext().ConfigureAwait(false);
                }
                else
                {
                    _messageQueue.Enqueue(nextMessage);
                }
            }
        }

        private async Task ExecuteMessage(IQueueMessage message)
        {
            using (message)
            {
                var details = JsonConvert.DeserializeObject<StoreDataDetails>(message.Message);

                bool hasData = false;
                if(details.Metadata.ContainsKey(MetadataConstants.TypeMetadataKey))
                {
                    var type = details.Metadata[MetadataConstants.TypeMetadataKey];
                    if(_typeIndexers.ContainsKey(type))
                    {
                        var indexer = (IIndexer)_typeResolver(_typeIndexers[type]);
                        await indexer.Index(details).ConfigureAwait(false);
                        hasData = true;
                    }
                }
                
                if(!hasData)
                {
                    var key = _pathIndexers.Keys.Where(k => details.BasePath.StartsWith(k)).FirstOrDefault();
                    if (key != null)
                    {
                        var indexer = (IIndexer)_typeResolver(_pathIndexers[key]);
                        await indexer.Index(details).ConfigureAwait(false);
                        hasData = true;
                    }
                }

                if(!hasData)
                {
                    throw new InvalidOperationException("Could not find indexer for record: container=" + details.Container + ", path=" + details.BasePath + ":\r\n" + message.Message);
                }

                await message.Complete().ConfigureAwait(false);
            }
        }

        private sealed class LockFreeQueue<T>
        {
            private sealed class Node
            {
                public readonly T Item;
                public Node Next;
                public Node(T item)
                {
                    Item = item;
                }
            }
            private volatile Node _head;
            private volatile Node _tail;
            public LockFreeQueue()
            {
                _head = _tail = new Node(default(T));
            }
#pragma warning disable 420 // volatile semantics not lost as only by-ref calls are interlocked
            public void Enqueue(T item)
            {
                Node newNode = new Node(item);
                for (; ; )
                {
                    Node curTail = _tail;
                    if (Interlocked.CompareExchange(ref curTail.Next, newNode, null) == null)   //append to the tail if it is indeed the tail.
                    {
                        Interlocked.CompareExchange(ref _tail, newNode, curTail);   //CAS in case we were assisted by an obstructed thread.
                        return;
                    }
                    else
                    {
                        Interlocked.CompareExchange(ref _tail, curTail.Next, curTail);  //assist obstructing thread.
                    }
                }
            }
            public bool TryDequeue(out T item)
            {
                for (; ; )
                {
                    Node curHead = _head;
                    Node curTail = _tail;
                    Node curHeadNext = curHead.Next;
                    if (curHead == curTail)
                    {
                        if (curHeadNext == null)
                        {
                            item = default(T);
                            return false;
                        }
                        else
                            Interlocked.CompareExchange(ref _tail, curHeadNext, curTail);   // assist obstructing thread
                    }
                    else
                    {
                        item = curHeadNext.Item;
                        if (Interlocked.CompareExchange(ref _head, curHeadNext, curHead) == curHead)
                        {
                            return true;
                        }
                    }
                }
            }
#pragma warning restore 420
        }
    }
}
