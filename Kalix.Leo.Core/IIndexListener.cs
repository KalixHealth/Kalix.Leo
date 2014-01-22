using System;

namespace Kalix.Leo
{
    public interface IIndexListener
    {
        void RegisterFallbackIndexer(Kalix.Leo.Indexing.IIndexer indexer);
        void RegisterTypeIndexer<T>(Kalix.Leo.Indexing.IIndexer indexer);
        IDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null);
    }
}
