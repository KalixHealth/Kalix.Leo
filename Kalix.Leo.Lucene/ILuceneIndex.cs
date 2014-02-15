using Lucene.Net.Analysis;
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
        IObservable<Document> SearchDocuments(Func<IndexSearcher, Analyzer, TopDocs> doSearchFunc);
        
        Task WriteToIndex(IObservable<Document> documents);
        Task WriteToIndex(Action<IndexWriter> writeUsingIndex);

        Task DeleteAll();
    }
}
