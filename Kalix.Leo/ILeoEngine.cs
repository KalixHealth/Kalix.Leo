using Kalix.Leo.Indexing;

namespace Kalix.Leo
{
    public interface ILeoEngine
    {
        IRecordSearchComposer Composer { get; }

        IDocumentPartition GetDocumentPartition(string basePath, long partitionId);
        IObjectPartition<T> GetObjectPartition<T>(long partitionId) where T : ObjectWithId;
        void StartListeners(int? messagesToProcessInParallel = null);
    }
}
