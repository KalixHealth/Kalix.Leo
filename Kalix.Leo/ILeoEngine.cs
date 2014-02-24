using Kalix.Leo.Indexing;

namespace Kalix.Leo
{
    public interface ILeoEngine
    {
        IRecordSearchComposer Composer { get; }

        IDocumentPartition GetDocumentPartition(string basePath, long partitionId);
        IObjectPartition<T> GetObjectPartition<T>(long partitionId);
        void StartListeners(int? messagesToProcessInParallel = null);
    }
}
