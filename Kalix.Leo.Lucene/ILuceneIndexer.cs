using Lucene.Net.Documents;
using Lucene.Net.Search;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    public interface ILuceneIndexer
    {
        IObservable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc);
        Task WriteToIndex(IObservable<Document> documents);

        Task DeleteAll();
    }
}
