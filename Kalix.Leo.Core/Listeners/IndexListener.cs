using Kalix.Leo.Indexing;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reflection;
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
            // Special queue system
            // We buffer up messages and execute either once every second, or wait until the previous execution has finished.
            // The messages in parrallel option will limit the total number of messages available
            var waitSubject = new Subject<Unit>();
            var subcr = _indexQueue.ListenForMessages(uncaughtException, messagesToProcessInParallel)
                .Buffer(waitSubject)
                .SelectMany(async (l) =>
                {
                    if (l.Count == 0)
                    {
                        // wait 1 sec till we try again...
                        await Task.Delay(1000);
                    }
                    else
                    {
                        // parse all groups in parrallel
                        await Task.WhenAll(l.GroupBy(m => FindKey(m)).Select(g => ExecuteMessages(g, uncaughtException)));
                    }

                    // We can execute the next set
                    waitSubject.OnNext(Unit.Default);
                    return Unit.Default;
                })
                .Catch<Unit, Exception>(e => 
                {
                    // Start a timer that will start the next buffer call when the whole thing has been repeated
                    Task.Delay(1000).ContinueWith(_ => waitSubject.OnNext(Unit.Default));
                    return Observable.Empty<Unit>(); 
                })
                .Repeat()
                .Subscribe();

            // Do the first hit...
            waitSubject.OnNext(Unit.Default);

            return subcr;
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
