using Kalix.Leo.Azure.Storage;
using Kalix.Leo.Storage;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Tests
{
    [TestFixture]
    public class LuceneIndexTests
    {
        protected IOptimisticStore _store;
        protected LuceneIndex _indexer;

        [SetUp]
        public void Init()
        {
            var client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient();
            _store = new AzureStore(client, false, null); // Do not snapshot and do tracing!
            _store.CreateContainerIfNotExists("testindexer");

            var store = new SecureStore(_store);
            _indexer = new LuceneIndex(store, "testindexer", "basePath", null);
        }

        [TearDown]
        public void TearDown()
        {
            _indexer.DeleteAll().Wait();
            _indexer.Dispose();
        }

        [Test]
        public void CannotWriteTwoLuceneIndexesOnSameStore()
        {
            _indexer.DeleteAll().Wait();

            var store = new SecureStore(_store);
            using (var indexer2 = new LuceneIndex(store, "testindexer", "basePath", null))
            {
                Assert.Throws<LockObtainFailedException>(() =>
                {
                    indexer2.DeleteAll().Wait();
                });
            }
        }

        [Test]
        public void Write100000EntriesAndSearchAtSameTimeNoErrors()
        {
            var docs = CreateIpsumDocs(100000);

            string error = null;
            int numDocs = 0;

            using (var reading = AsyncEnumerableEx.CreateTimer(TimeSpan.FromSeconds(0.5))
                .Select(t => _indexer.SearchDocuments(i =>
                {
                    var query = new TermQuery(new Term("words", "ipsum"));
                    return i.Search(query, 20);
                }))
                .Select(d => numDocs += d.Count())
                .TakeUntilDisposed(null, t => { if (t.IsFaulted) { error = t.Exception.GetBaseException().Message; } }))
            {
                _indexer.WriteToIndex(docs, true).Wait();
            }

            Assert.AreEqual(null, error);
        }

        [Test]
        public void CanWriteFromSameIndexerConcurrently()
        {
            var task1 = _indexer.WriteToIndex(CreateIpsumDocs(30000));
            var task2 = _indexer.WriteToIndex(CreateIpsumDocs(30000));

            Task.WhenAll(task1, task2).Wait();
        }

        [Test]
        public void CanReadFromTwoIndexes()
        {
            _indexer.WriteToIndex(CreateIpsumDocs(30000)).Wait();

            var t1 = Task.Run(() =>
            {
                var parser = new TermQuery(new Term("words", "ipsum"));
                var stream1 = _indexer.SearchDocuments(s => s.Search(parser, 10000));
            });

            var t2 = Task.Run(() =>
            {
                var parser2 = new TermQuery(new Term("words", "lorem"));
                var stream2 = _indexer.SearchDocuments(s => s.Search(parser2, 10000));
            });

            Task.WaitAll(t1, t2);
        }

        [Test]
        public void EmptyIndexReturnsNoDocuments()
        {
            var stream1 = _indexer.SearchDocuments(s => s.Search(new MatchAllDocsQuery(), int.MaxValue));

            Assert.AreEqual(0, stream1.Count());
        }

        [Test]
        public void TwoIndexesOneRefreshesTheOther()
        {
            LeoTrace.WriteLine = (s) => Trace.WriteLine(s);

            _indexer.WriteToIndex(CreateIpsumDocs(2000), true).Wait();

            var number = _indexer.SearchDocuments(ind =>
            {
                var query = new TermQuery(new Term("words", "ipsum"));
                return ind.Search(query, 20);
            }).ToList();

            Assert.Greater(number.Count, 0);
        }

        [Test]
        public void CanDisposeAndRead()
        {
            _indexer.WriteToIndex(CreateIpsumDocs(2000), true).Wait();
            _indexer.Dispose();
            _indexer = new LuceneIndex(new SecureStore(_store), "testindexer", "basePath", null);

            var number = _indexer.SearchDocuments(ind =>
            {
                var query = new TermQuery(new Term("words", "ipsum"));
                return ind.Search(query, 20);
            }).ToList();

            Assert.Greater(number.Count, 0);
        }

        [Test]
        public void RecreatingWriterDoesNotRefreshIndex()
        {
            LeoTrace.WriteLine = Console.WriteLine;

            _indexer.WriteToIndex(CreateIpsumDocs(3), true).Wait();
            _indexer.Dispose();
            _indexer = new LuceneIndex(new SecureStore(_store), "testindexer", "basePath", null);
            _indexer.WriteToIndex(CreateIpsumDocs(3), true).Wait();
            _indexer.Dispose();
            _indexer = new LuceneIndex(new SecureStore(_store), "testindexer", "basePath", null);

            var number = _indexer.SearchDocuments(s => s.Search(new MatchAllDocsQuery(), int.MaxValue)).Count();
            Assert.AreEqual(6, number);
        }

        private IEnumerable<Document> CreateIpsumDocs(int number)
        {
            return Enumerable.Range(0, number)
                .Select(i =>
                {
                    if(i % 10000 == 0)
                    {
                        Trace.WriteLine("Writing doc: " + i);
                    }

                    var doc = new Document();
                    doc.Add(new NumericField("id").SetIntValue(i));
                    doc.Add(new Field("words", Ipsum.GetPhrase(20), Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO));
                    return doc;
                });
        }
    }
}
