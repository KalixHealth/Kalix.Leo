using Kalix.Leo.Encryption;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kalix.Leo.Indexing
{
    public interface IRecordSearchComposition<TMain, TSearch>
    {
        Task Save(long partitionKey, string id, TMain item, TMain previous, IEncryptor encryptor);
        Task Delete(long partitionKey, string id, TMain main);

        IObservable<TSearch> Search(long partitionKey, IEncryptor encryptor, IRecordSearch search);

        IObservable<TSearch> SearchFor<T1>(long partitionKey, IEncryptor encryptor, IRecordSearch<T1> search, T1 val);
        IObservable<TSearch> SearchFor<T1, T2>(long partitionKey, IEncryptor encryptor, IRecordSearch<T1, T2> search, T1 val);

        IObservable<TSearch> SearchBetween<T1>(long partitionKey, IEncryptor encryptor, IRecordSearch<T1> search, T1 start, T1 end);

        IObservable<TSearch> SearchBetween<T1, T2>(long partitionKey, IEncryptor encryptor, IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end);
    }
}
