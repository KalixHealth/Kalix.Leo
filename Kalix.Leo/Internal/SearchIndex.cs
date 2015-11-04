using Kalix.Leo.Encryption;
using Kalix.Leo.Indexing;
using Kalix.Leo.Storage;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

namespace Kalix.Leo.Internal
{
    public class SearchIndex<TMain, TSearch> : ISearchIndex<TMain, TSearch>
    {
        private readonly long _partitionId;
        private readonly IEncryptor _encryptor;
        private readonly IRecordSearchComposition<TMain, TSearch> _composition;

        public SearchIndex(IRecordSearchComposition<TMain, TSearch> composition, IEncryptor encryptor, long partitionId)
        {
            _encryptor = encryptor;
            _partitionId = partitionId;
            _composition = composition;
        }

        public Task Save(string id, ObjectWithMetadata<TMain> item, ObjectWithMetadata<TMain> previous)
        {
            return _composition.Save(_partitionId, id, item, previous, _encryptor);
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

        public IAsyncEnumerable<TSearch> SearchBetween<T1>(IRecordSearch<T1> search, T1 start, T1 end)
        {
            return _composition.SearchBetween(_partitionId, _encryptor, search, start, end);
        }

        public IAsyncEnumerable<TSearch> SearchBetween<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end)
        {
            return _composition.SearchBetween(_partitionId, _encryptor, search, val, start, end);
        }
    }
}
