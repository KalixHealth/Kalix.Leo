namespace Kalix.Leo
{
    public interface ILeoEngine
    {
        IDocumentPartition GetDocumentPartition(string basePath, string container);
        IObjectPartition<T> GetObjectPartition<T>(string container) where T : ObjectWithId;
        void StartListeners(int? messagesToProcessInParallel = null);
    }
}
