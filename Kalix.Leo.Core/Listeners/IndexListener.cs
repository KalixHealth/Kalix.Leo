using Kalix.Leo.Indexing;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Listeners
{
    public class IndexListener : IIndexListener
    {
        private readonly IQueue _indexQueue;
        private readonly Dictionary<string, IIndexer> _typeIndexers;
        private readonly Dictionary<string, IIndexer> _pathIndexers;
        private readonly Func<string, Type> _typeResolver;

        public IndexListener(IQueue indexQueue, Func<string, Type> typeResolver = null)
        {
            _indexQueue = indexQueue;
            _typeIndexers = new Dictionary<string, IIndexer>();
            _pathIndexers = new Dictionary<string, IIndexer>();
            _typeResolver = typeResolver ?? (s => Type.GetType(s, false));
        }

        public void RegisterPathIndexer(string basePath, IIndexer indexer)
        {
            if (_pathIndexers.ContainsKey(basePath))
            {
                throw new InvalidOperationException("Already have a indexer for base path: " + basePath);
            }

            _pathIndexers[basePath] = indexer;
        }

        public void RegisterTypeIndexer<T>(IIndexer indexer)
        {
            RegisterTypeIndexer(typeof(T), indexer);
        }

        public void RegisterTypeIndexer(Type type, IIndexer indexer)
        {
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
                        await _typeIndexers[type].Index(details);
                        hasData = true;
                    }
                }
                
                if(!hasData)
                {
                    var key = _pathIndexers.Keys.Where(k => details.BasePath.StartsWith(k)).FirstOrDefault();
                    if (key != null)
                    {
                        await _pathIndexers[key].Index(details);
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
