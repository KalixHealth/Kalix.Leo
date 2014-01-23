using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    public interface ILuceneIndex
    {
        IObservable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc);
        
        Task WriteToIndex(IObservable<Document> documents);
        Task WriteToIndex(Action<IndexWriter> writeUsingIndex);

        Task DeleteAll();
    }
}
