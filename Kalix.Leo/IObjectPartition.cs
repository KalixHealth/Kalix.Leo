using Kalix.Leo.Storage;
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface IObjectPartition<T> : IBasePartition
    {
        Task Save(T data, long id, Metadata metadata = null);
        Task<long> Save(T data, Expression<Func<T, long?>> idField, Action<long> preSaveProcessing = null, Metadata metadata = null);
        Task<ObjectWithMetadata<T>> Load(long id, string snapshot = null);
        Task<Metadata> GetMetadata(long id, string snapshot = null);

        IObservable<Snapshot> FindSnapshots(long id);
        IObservable<IdWithMetadata> FindAll();

        Task Delete(long id);
        Task DeletePermanent(long id);

        Task ReIndexAll();
        Task ReBackupAll();

        Task SetInternalIdGenerator(long newId);
    }
}
