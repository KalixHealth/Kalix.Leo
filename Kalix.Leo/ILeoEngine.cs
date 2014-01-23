namespace Kalix.Leo
{
    public interface ILeoEngine
    {
        IDocumentPartition GetDocumentPartition(string basePath, string container);
        IObjectPartition<T> GetObjectPartition<T>(string container);
        void StartListeners(int? messagesToProcessInParallel = null);
    }
}
