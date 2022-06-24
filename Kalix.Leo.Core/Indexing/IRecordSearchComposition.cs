using Kalix.Leo.Encryption;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kalix.Leo.Indexing;

public interface IRecordSearchComposition<TMain, TSearch>
{
    Task Save(long partitionKey, string id, ObjectWithMetadata<TMain> item, ObjectWithMetadata<TMain> previous, IEncryptor encryptor);
    Task Delete(long partitionKey, string id, ObjectWithMetadata<TMain> main);

    Task<bool> IndexExists<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordUniqueIndex<T1> index, T1 val);

    IAsyncEnumerable<TSearch> SearchAll(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch search);
    IAsyncEnumerable<TSearch> SearchAll<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1> search);
    IAsyncEnumerable<TSearch> SearchAll<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search);

    IAsyncEnumerable<TSearch> SearchFor<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1> search, T1 val);
    IAsyncEnumerable<TSearch> SearchFor<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search, T1 val);
    IAsyncEnumerable<TSearch> SearchFor<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search, T1 val, T2 val2);

    IAsyncEnumerable<TSearch> SearchBetween<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1> search, T1 start, T1 end);

    IAsyncEnumerable<TSearch> SearchBetween<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end);

    Task<int> CountAll(long partitionKey, IRecordSearch search);
    Task<int> CountAll<T1>(long partitionKey, IRecordSearch<T1> search);
    Task<int> CountAll<T1, T2>(long partitionKey, IRecordSearch<T1, T2> search);

    Task<int> CountFor<T1>(long partitionKey, IRecordSearch<T1> search, T1 val);
    Task<int> CountFor<T1, T2>(long partitionKey, IRecordSearch<T1, T2> search, T1 val);
    Task<int> CountFor<T1, T2>(long partitionKey, IRecordSearch<T1, T2> search, T1 val, T2 val2);

    Task<int> CountBetween<T1>(long partitionKey, IRecordSearch<T1> search, T1 start, T1 end);

    Task<int> CountBetween<T1, T2>(long partitionKey, IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end);
}