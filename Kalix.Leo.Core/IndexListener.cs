using Kalix.Leo.Indexing;
using Kalix.Leo.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class IndexListener : IIndexListener
    {
        private readonly IQueue _indexQueue;
        private readonly Dictionary<string, IIndexer> _indexers;
        private readonly Func<string, Type> _typeResolver;
        private IIndexer _fallback;

        public IndexListener(IQueue indexQueue, Func<string, Type> typeResolver = null)
        {
            _indexQueue = indexQueue;
            _indexers = new Dictionary<string, IIndexer>();
            _typeResolver = typeResolver ?? (s => Type.GetType(s, false));
        }

        public void RegisterFallbackIndexer(IIndexer indexer)
        {
            if(_fallback != null)
            {
                throw new InvalidOperationException("Already have a fallback indexer");
            }

            _fallback = indexer;
        }

        public void RegisterTypeIndexer<T>(IIndexer indexer)
        {
            var type = typeof(T).FullName;
            if (_indexers.ContainsKey(type)) 
            {
                throw new InvalidOperationException("Already have a indexer for type: " + type);
            }

            _indexers[type] = indexer;
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
                    if(_indexers.ContainsKey(type))
                    {
                        await _indexers[type].Index(details);
                        hasData = true;
                    }
                }
                
                if(!hasData)
                {
                    if(_fallback == null)
                    {
                        throw new InvalidOperationException("No fallback indexer is registered, data could not be indexed: container=" + details.Container);
                    }

                    await _fallback.Index(details);
                }

                await message.Complete();
            }
        }
    }
}
