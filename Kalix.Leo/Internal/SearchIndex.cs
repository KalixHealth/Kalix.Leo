using Kalix.Leo.Encryption;
using Kalix.Leo.Indexing;
using System;
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

        public Task Save(string id, TMain item, TMain previous)
        {
            return _composition.Save(_partitionId, id, item, previous, _encryptor);
        }

        public Task Delete(string id, TMain main)
        {
            return _composition.Delete(_partitionId, id, main);
        }

        public IObservable<TSearch> Search(IRecordSearch search)
        {
            return _composition.Search(_partitionId, _encryptor, search);
        }

        public IObservable<TSearch> SearchFor<T1>(IRecordSearch<T1> search, T1 val)
        {
            return _composition.SearchFor(_partitionId, _encryptor, search, val);
        }

        public IObservable<TSearch> SearchFor<T1, T2>(IRecordSearch<T1, T2> search, T1 val)
        {
            return _composition.SearchFor(_partitionId, _encryptor, search, val);
        }

        public IObservable<TSearch> SearchBetween<T1>(IRecordSearch<T1> search, T1 start, T1 end)
        {
            return _composition.SearchBetween(_partitionId, _encryptor, search, start, end);
        }

        public IObservable<TSearch> SearchBetween<T1, T2>(IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end)
        {
            return _composition.SearchBetween(_partitionId, _encryptor, search, val, start, end);
        }
    }
}
