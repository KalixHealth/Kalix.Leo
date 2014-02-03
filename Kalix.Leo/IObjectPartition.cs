using Kalix.Leo.Storage;
using Lucene.Net.Search;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface IObjectPartition<T> : IBasePartition
        where T : ObjectWithId
    {
        Task<long> Save(T data, IMetadata metadata = null);
        Task<ObjectWithMetadata<T>> Load(long id, string snapshot = null);
        Task<IMetadata> GetMetadata(long id, string snapshot = null);

        IObservable<Snapshot> FindSnapshots(long id);
        IObservable<IdWithMetadata> FindAll();

        Task Delete(long id);

        Task ReIndexAll();
        Task ReBackupAll();
    }
}
