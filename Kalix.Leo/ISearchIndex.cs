using Kalix.Leo.Indexing;
using Kalix.Leo.Storage;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface ISearchIndex<TMain, TSearch>
    {
        Task Save(string id, ObjectWithMetadata<TMain> item, ObjectWithMetadata<TMain> previous);
        Task Save(long id, ObjectWithMetadata<TMain> item, ObjectWithMetadata<TMain> previous);
        Task Delete(string id, ObjectWithMetadata<TMain> current);
        Task Delete(long id, ObjectWithMetadata<TMain> current);

        Task<TSearch> GetByIndex<T1>(IRecordUniqueIndex<T1> index, T1 val);

        IAsyncEnumerable<TSearch> SearchAll(IRecordSearch search);
        IAsyncEnumerable<TSearch> SearchAll<T1>(IRecordSearch<T1> search);
        IAsyncEnumerable<TSearch> SearchAll<T1, T2>(IRecordSearch<T1, T2> search);

        IAsyncEnumerable<TSearch> SearchFor<T1>(IRecordSearch<T1> search, T1 val);
        IAsyncEnumerable<TSearch> SearchFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val);
        IAsyncEnumerable<TSearch> SearchFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 val2);

        IAsyncEnumerable<TSearch> SearchBetween<T1>(IRecordSearch<T1> search, T1 start, T1 end);

        IAsyncEnumerable<TSearch> SearchBetween<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end);

        Task<int> CountAll(IRecordSearch search);
        Task<int> CountAll<T1>(IRecordSearch<T1> search);
        Task<int> CountAll<T1, T2>(IRecordSearch<T1, T2> search);

        Task<int> CountFor<T1>(IRecordSearch<T1> search, T1 val);
        Task<int> CountFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val);
        Task<int> CountFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 val2);

        Task<int> CountBetween<T1>(IRecordSearch<T1> search, T1 start, T1 end);

        Task<int> CountBetween<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end);
    }
}
