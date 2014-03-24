using System;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface IDocumentPartition : IBasePartition
    {
        Task Save(string path, IObservable<byte[]> data, Metadata metadata = null);
        Task<DataWithMetadata> Load(string path, string snapshot = null);
        Task<Metadata> GetMetadata(string path, string snapshot = null);

        IObservable<Snapshot> FindSnapshots(string path);
        IObservable<PathWithMetadata> Find(string prefix = null);

        Task Delete(string path);
        Task DeletePermanent(string path);

        Task ReIndexAll();
        Task ReBackupAll();
    }
}
