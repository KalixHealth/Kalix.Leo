using Kalix.Leo.Indexing;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
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
            return _indexQueue.ListenForMessages(uncaughtException, messagesToProcessInParallel)
                .Select(m => Observable
                    .FromAsync(() => MessageRecieved(m))
                    .Catch((Func<Exception, IObservable<Unit>>)(e =>
                    {
                        if (uncaughtException != null) { uncaughtException(e); }
                        return Observable.Empty<Unit>();
                    }))) // Make sure this listener doesnt stop due to errors!
                .Merge()
                .Subscribe(); // Start listening
        }

        private async Task MessageRecieved(IQueueMessage message)
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
                        await indexer.Index(details);
                        hasData = true;
                    }
                }
                
                if(!hasData)
                {
                    var key = _pathIndexers.Keys.Where(k => details.BasePath.StartsWith(k)).FirstOrDefault();
                    if (key != null)
                    {
                        var indexer = (IIndexer)_typeResolver(_pathIndexers[key]);
                        await indexer.Index(details);
                        hasData = true;
                    }
                }

                if(!hasData)
                {
                    throw new InvalidOperationException("Could not find indexer for record: container=" + details.Container + ", path=" + details.BasePath);
                }

                await message.Complete();
            }
        }
    }
}
