using Kalix.Leo.Indexing;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface ISearchIndex<TMain, TSearch>
    {
        Task Save(string id, TMain item, TMain previous);
        Task Save(long id, TMain item, TMain previous);
        Task Delete(string id, TMain current);
        Task Delete(long id, TMain current);

        IObservable<TSearch> SearchAll(IRecordSearch search);
        IObservable<TSearch> SearchAll<T1>(IRecordSearch<T1> search);
        IObservable<TSearch> SearchAll<T1, T2>(IRecordSearch<T1, T2> search);

        IObservable<TSearch> SearchFor<T1>(IRecordSearch<T1> search, T1 val);
        IObservable<TSearch> SearchFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val);

        IObservable<TSearch> SearchBetween<T1>(IRecordSearch<T1> search, T1 start, T1 end);

        IObservable<TSearch> SearchBetween<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end);
    }
}
