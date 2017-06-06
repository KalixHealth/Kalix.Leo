using Lucene.Net.Analysis;
using Lucene.Net.Contrib.Management;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using System;
using System.Collections.Generic;
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
        /// <param name="forceCheck">Whether to always check the index for any updates, less performant if true</param>
        /// <returns>A list of documents derived from the topdocs</returns>
        IEnumerable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc, bool forceCheck = false);

        /// <summary>
        /// Search for documents using lucene, you have to supply the actual search with the provided searcher
        /// </summary>
        /// <param name="doSearchFunc">Use a searcher and the underlying analyzer to find a list of topdocs</param>
        /// <param name="forceCheck">Whether to always check the index for any updates, less performant if true</param>
        /// <returns>A list of documents derived from the topdocs</returns>
        IEnumerable<Document> SearchDocuments(Func<IndexSearcher, Analyzer, TopDocs> doSearchFunc, bool forceCheck = false);
        
        /// <summary>
        /// Write a number of documents to the index at once
        /// </summary>
        /// <param name="documents">The documents to load into the index</param>
        /// <param name="waitForGeneration">Should the writer wait for a generation</param>
        /// <returns>A task that is complete when all the documents have been indexed</returns>
        Task WriteToIndex(IEnumerable<Document> documents, bool waitForGeneration = false);

        /// <summary>
        /// Write documents with complete access to the lucene index
        /// </summary>
        /// <param name="writeUsingIndex">Do your indexing with the provided index</param>
        /// <returns>A task that is complete when all the documents have been indexed</returns>
        Task WriteToIndex(Func<NrtManager, Task> writeUsingIndex);

        /// <summary>
        /// Delete the entire index
        /// </summary>
        /// <returns>A task when all records have been deleted</returns>
        Task DeleteAll();
    }
}
