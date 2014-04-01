using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    /// <summary>
    /// A wrapper interface that handles the lifetime of writers and readers
    /// </summary>
    public interface ILuceneIndex
    {
        /// <summary>
        /// Search for documents using lucene, you have to supply the actual search with the provided searcher
        /// </summary>
        /// <param name="doSearchFunc">Use a searcher to find a list of topdocs</param>
        /// <returns>A list of documents derived from the topdocs</returns>
        IObservable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc);

        /// <summary>
        /// Search for documents using lucene, you have to supply the actual search with the provided searcher
        /// </summary>
        /// <param name="doSearchFunc">Use a searcher and the underlying analyzer to find a list of topdocs</param>
        /// <returns>A list of documents derived from the topdocs</returns>
        IObservable<Document> SearchDocuments(Func<IndexSearcher, Analyzer, TopDocs> doSearchFunc);
        
        /// <summary>
        /// Write a number of documents to the index at once
        /// </summary>
        /// <param name="documents">The documents to load into the index</param>
        /// <returns>A task that is complete when all the documents have been indexed</returns>
        Task WriteToIndex(IObservable<Document> documents);

        /// <summary>
        /// Write documents with complete access to the lucene index
        /// </summary>
        /// <param name="writeUsingIndex">Do your indexing with the provided index</param>
        /// <returns>A task that is complete when all the documents have been indexed</returns>
        Task WriteToIndex(Action<IndexWriter> writeUsingIndex);

        /// <summary>
        /// Delete the entire index
        /// </summary>
        /// <returns>A task when all records have been deleted</returns>
        Task DeleteAll();
    }
}
