using Kalix.Leo.Indexing;
using System;

namespace Kalix.Leo.Listeners
{
    public interface IIndexListener
    {
        void RegisterPathIndexer(string basePath, Type indexer);
        
        void RegisterTypeIndexer<T>(Type indexer);
        void RegisterTypeIndexer(Type type, Type indexer);

        IDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null);
    }
}
