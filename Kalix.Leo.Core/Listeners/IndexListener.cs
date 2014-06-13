using Kalix.Leo.Indexing;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
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

        private LockFreeQueue<IQueueMessage> _messageQueue;
        private ConcurrentDictionary<string, bool> _currentContainers;

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
            var disposable = new CompositeDisposable();

            var process = Observable.Create<Unit>(obs =>
            {
                var cancel = new CancellationDisposable();

                Task.Run(async () =>
                {
                    while (!cancel.Token.IsCancellationRequested)
                    {
                        if (_messageQueue.ApproxCount > 0)
                        {
                            for (int i = 0; i < _messageQueue.ApproxCount; i++)
                            {
                                #pragma warning disable 4014
                                Task.Run(() => TryExecuteNext(uncaughtException));
                            }
                        }

                        LeoTrace.WriteLine("Waiting to poll enqueued index items");
                        await Task.Delay(TimeSpan.FromSeconds(2));
                    }
                }, cancel.Token);

                // Return the item to dispose when done
                return cancel;
            }).SubscribeOn(NewThreadScheduler.Default);

            disposable.Add(process
                .Catch((Func<Exception, IObservable<Unit>>)(e =>
                {
                    // If we hit an error flush the queue...
                    var oldQueue = _messageQueue;
                    _messageQueue = new LockFreeQueue<IQueueMessage>();
                    _currentContainers = new ConcurrentDictionary<string, bool>();

                    IQueueMessage message;
                    while (oldQueue.TryDequeue(out message))
                    {
                        message.Dispose();
                    }

                    LeoTrace.WriteLine("Index listener error caught: " + e.Message);
                    if (uncaughtException != null) { uncaughtException(e); }
                    return Observable.Empty<Unit>();
                }))
                .Repeat()
                .Subscribe());

            disposable.Add(_indexQueue.ListenForMessages(uncaughtException, messagesToProcessInParallel)
                .Do(q => MessageRecieved(q))
                .Catch((Func<Exception, IObservable<IQueueMessage>>)(e =>
                {
                    LeoTrace.WriteLine("Index listener error caught: " + e.Message);
                    if (uncaughtException != null) { uncaughtException(e); }
                    return Observable.Empty<IQueueMessage>();
                }))
                .Repeat()
                .Subscribe());

            return disposable;
        }

        private void MessageRecieved(IQueueMessage message)
        {
            LeoTrace.WriteLine("Index listener message received");
            _messageQueue.Enqueue(message);
        }

        private async Task TryExecuteNext(Action<Exception> uncaughtException = null)
        {
            IQueueMessage nextMessage;
            if(_messageQueue.TryDequeue(out nextMessage))
            {
                try
                {
                    var details = JsonConvert.DeserializeObject<StoreDataDetails>(nextMessage.Message);
                    var firstPath = details.BasePath.Split(new char[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries)[0];
                    var key = details.Container + "_" + firstPath;

                    if (_currentContainers.TryAdd(key, true))
                    {
                        try
                        {
                            await ExecuteMessage(nextMessage).ConfigureAwait(false);
                            LeoTrace.WriteLine("Index message handled");
                        }
                        catch (Exception e)
                        {
                            LeoTrace.WriteLine("Index listener error caught: " + e.Message);
                            if (uncaughtException != null) { uncaughtException(e); }
                        }
                        finally
                        {
                            bool temp;
                            _currentContainers.TryRemove(key, out temp);
                            nextMessage.Dispose();
                        }
                    }
                    else
                    {
                        _messageQueue.Enqueue(nextMessage);
                    }
                }
                catch(Exception e)
                {
                    LeoTrace.WriteLine("Index listener error caught: " + e.Message);
                    if (uncaughtException != null) { uncaughtException(e); }
                    nextMessage.Dispose();
                }
            }
        }

        private async Task ExecuteMessage(IQueueMessage message)
        {
            try
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
            finally
            {
                message.Dispose();
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
            private int _approxCount;
            public LockFreeQueue()
            {
                _head = _tail = new Node(default(T));
                _approxCount = 0;
            }

            public int ApproxCount { get { return _approxCount; } }

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
                        Interlocked.Increment(ref _approxCount);
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
                            Interlocked.Decrement(ref _approxCount);
                            return true;
                        }
                    }
                }
            }
#pragma warning restore 420
        }
    }
}
