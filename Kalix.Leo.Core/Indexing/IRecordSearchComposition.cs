using Kalix.Leo.Encryption;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kalix.Leo.Indexing
{
    public interface IRecordSearchComposition<TMain, TSearch>
    {
        Task Save(long partitionKey, string id, ObjectWithMetadata<TMain> item, ObjectWithMetadata<TMain> previous, IEncryptor encryptor);
        Task Delete(long partitionKey, string id, ObjectWithMetadata<TMain> main);

        IAsyncEnumerable<TSearch> SearchAll(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch search);
        IAsyncEnumerable<TSearch> SearchAll<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1> search);
        IAsyncEnumerable<TSearch> SearchAll<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search);

        IAsyncEnumerable<TSearch> SearchFor<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1> search, T1 val);
        IAsyncEnumerable<TSearch> SearchFor<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search, T1 val);
        IAsyncEnumerable<TSearch> SearchFor<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search, T1 val, T2 val2);

        IAsyncEnumerable<TSearch> SearchBetween<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1> search, T1 start, T1 end);

        IAsyncEnumerable<TSearch> SearchBetween<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end);
    }
}
