using Kalix.Leo.Azure.Storage;
using Kalix.Leo.Storage;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
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
        [ExpectedException(typeof(LockObtainFailedException))]
        public void CannotWriteTwoLuceneIndexesOnSameStore()
        {
            _indexer.DeleteAll().Wait();

            var store = new SecureStore(_store);
            using (var indexer2 = new LuceneIndex(store, "testindexer", "basePath", null))
            {
                indexer2.DeleteAll().Wait();
            }
        }

        [Test]
        public void Write100000EntriesAndSearchAtSameTimeNoErrors()
        {
            var docs = CreateIpsumDocs(100000);

            string error = null;
            int numDocs = 0;

            using (var reading = Observable.Interval(TimeSpan.FromSeconds(0.5))
                .SelectMany(t => _indexer.SearchDocuments(i =>
                {
                    var query = new TermQuery(new Term("words", "ipsum"));
                    return i.Search(query, 20);
                }))
                .Subscribe(d => { numDocs++; }, e => { error = e.GetBaseException().Message; }, () => { }))
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

            var parser = new TermQuery(new Term("words", "ipsum"));
            var stream1 = _indexer.SearchDocuments(s => s.Search(parser, 20));

            var parser2 = new TermQuery(new Term("words", "lorem"));
            var stream2 = _indexer.SearchDocuments(s => s.Search(parser2, 20));

            stream1
                .Merge(stream2)
                .Take(30000)
                .LastOrDefaultAsync()
                .Wait();
        }

        [Test]
        public void EmptyIndexReturnsNoDocuments()
        {
            var stream1 = _indexer.SearchDocuments(s => s.Search(new MatchAllDocsQuery(), int.MaxValue));

            Assert.AreEqual(0, stream1.Count().ToEnumerable().First());
        }

        [Test]
        public void TwoIndexesOneRefreshesTheOther()
        {
            LeoTrace.WriteLine = (s) => Trace.WriteLine(s);

            _indexer.WriteToIndex(CreateIpsumDocs(2000), true).Wait();

            var number = Task.Run(async () => await _indexer.SearchDocuments(ind =>
            {
                var query = new TermQuery(new Term("words", "ipsum"));
                return ind.Search(query, 20);
            }).ToList()).Result;

            Assert.Greater(number.Count, 0);
        }

        [Test]
        public void CanDisposeAndRead()
        {
            _indexer.WriteToIndex(CreateIpsumDocs(2000), true).Wait();
            _indexer.Dispose();
            _indexer = new LuceneIndex(new SecureStore(_store), "testindexer", "basePath", null);

            var number = Task.Run(async () => await _indexer.SearchDocuments(ind =>
            {
                var query = new TermQuery(new Term("words", "ipsum"));
                return ind.Search(query, 20);
            }).ToList()).Result;

            Assert.Greater(number.Count, 0);
        }

        private IObservable<Document> CreateIpsumDocs(int number)
        {
            return Observable.Range(0, number)
                .ObserveOn(Scheduler.Default)
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
