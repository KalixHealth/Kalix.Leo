using Kalix.Leo.Encryption;
using Kalix.Leo.Indexing;
using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

namespace Kalix.Leo.Internal
{
    public class SearchIndex<TMain, TSearch> : ISearchIndex<TMain, TSearch>
    {
        private readonly long _partitionId;
        private readonly Lazy<Task<IEncryptor>> _encryptor;
        private readonly IRecordSearchComposition<TMain, TSearch> _composition;

        public SearchIndex(IRecordSearchComposition<TMain, TSearch> composition, Lazy<Task<IEncryptor>> encryptor, long partitionId)
        {
            _encryptor = encryptor ?? new Lazy<Task<IEncryptor>>(() => Task.FromResult((IEncryptor)null));
            _partitionId = partitionId;
            _composition = composition;
        }

        public async Task Save(string id, ObjectWithMetadata<TMain> item, ObjectWithMetadata<TMain> previous)
        {
            var encryptor = await _encryptor.Value;
            await _composition.Save(_partitionId, id, item, previous, encryptor);
        }

        public Task Save(long id, ObjectWithMetadata<TMain> item, ObjectWithMetadata<TMain> previous)
        {
            return Save(id.ToString(CultureInfo.InvariantCulture), item, previous);
        }

        public Task Delete(string id, ObjectWithMetadata<TMain> current)
        {
            return _composition.Delete(_partitionId, id, current);
        }

        public Task Delete(long id, ObjectWithMetadata<TMain> current)
        {
            return Delete(id.ToString(CultureInfo.InvariantCulture), current);
        }

        public Task<bool> IndexExists<T1>(IRecordUniqueIndex<T1> index, T1 val)
        {
            return _composition.IndexExists(_partitionId, _encryptor, index, val);
        }

        public IAsyncEnumerable<TSearch> SearchAll(IRecordSearch search)
        {
            return _composition.SearchAll(_partitionId, _encryptor, search);
        }

        public IAsyncEnumerable<TSearch> SearchAll<T1>(IRecordSearch<T1> search)
        {
            return _composition.SearchAll(_partitionId, _encryptor, search);
        }

        public IAsyncEnumerable<TSearch> SearchAll<T1, T2>(IRecordSearch<T1, T2> search)
        {
            return _composition.SearchAll(_partitionId, _encryptor, search);
        }

        public IAsyncEnumerable<TSearch> SearchFor<T1>(IRecordSearch<T1> search, T1 val)
        {
            return _composition.SearchFor(_partitionId, _encryptor, search, val);
        }

        public IAsyncEnumerable<TSearch> SearchFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val)
        {
            return _composition.SearchFor(_partitionId, _encryptor, search, val);
        }

        public IAsyncEnumerable<TSearch> SearchFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 val2)
        {
            return _composition.SearchFor(_partitionId, _encryptor, search, val, val2);
        }

        public IAsyncEnumerable<TSearch> SearchBetween<T1>(IRecordSearch<T1> search, T1 start, T1 end)
        {
            return _composition.SearchBetween(_partitionId, _encryptor, search, start, end);
        }

        public IAsyncEnumerable<TSearch> SearchBetween<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end)
        {
            return _composition.SearchBetween(_partitionId, _encryptor, search, val, start, end);
        }

        public Task<int> CountAll(IRecordSearch search)
        {
            return _composition.CountAll(_partitionId, search);
        }

        public Task<int> CountAll<T1>(IRecordSearch<T1> search)
        {
            return _composition.CountAll(_partitionId, search);
        }

        public Task<int> CountAll<T1, T2>(IRecordSearch<T1, T2> search)
        {
            return _composition.CountAll(_partitionId, search);
        }

        public Task<int> CountFor<T1>(IRecordSearch<T1> search, T1 val)
        {
            return _composition.CountFor(_partitionId, search, val);
        }

        public Task<int> CountFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val)
        {
            return _composition.CountFor(_partitionId, search, val);
        }

        public Task<int> CountFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 val2)
        {
            return _composition.CountFor(_partitionId, search, val, val2);
        }

        public Task<int> CountBetween<T1>(IRecordSearch<T1> search, T1 start, T1 end)
        {
            return _composition.CountBetween(_partitionId, search, start, end);
        }

        public Task<int> CountBetween<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end)
        {
            return _composition.CountBetween(_partitionId, search, val, start, end);
        }
    }
}
