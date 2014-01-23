using Kalix.Leo.Indexing;
using System;

namespace Kalix.Leo.Listeners
{
    public interface IIndexListener
    {
        void RegisterPathIndexer(string basePath, IIndexer indexer);
        
        void RegisterTypeIndexer<T>(IIndexer indexer);
        void RegisterTypeIndexer(Type type, IIndexer indexer);

        IDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null);
    }
}
