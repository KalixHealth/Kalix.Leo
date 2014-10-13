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
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
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
        public void TwoSeperateIndexersWriteShouldFail()
        {
            var store = new SecureStore(_store);
            var indexer2 = new LuceneIndex(store, "testindexer", "basePath", null);

            var task1 = _indexer.WriteToIndex(CreateIpsumDocs(30000));
            var task2 = indexer2.WriteToIndex(CreateIpsumDocs(30000));

            try
            {
                Task.WaitAll(task1, task2);
            }
            catch(AggregateException e)
            {
                throw e.InnerException;
            }
        }

        [Test]
        public void TwoSeperateIndexersReadingAndWritingAtSameTimeNoErrors()
        {
            var store = new SecureStore(_store);
            var indexer2 = new LuceneIndex(store, "testindexer", "basePath", null);

            var docs = CreateIpsumDocs(100000);

            string error = null;
            int numDocs = 0;

            var writeTask = Task.Run(() =>
            {
                _indexer.WriteToIndex(docs).Wait();
            });

            Task.Run(async () =>
            {
                using (var reading = Observable.Interval(TimeSpan.FromSeconds(0.5))
                    .SelectMany(t => indexer2.SearchDocuments(i =>
                    {
                        var query = new TermQuery(new Term("words", "ipsum"));
                        return i.Search(query, 20);
                    }))
                    .Subscribe(d => { numDocs++; }, e => { error = e.GetBaseException().Message; }, () => { }))
                {
                    await writeTask;
                }
            }).Wait();

            Assert.AreEqual(null, error);
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
                _indexer.WriteToIndex(docs).Wait();
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
        public void IndexersManyWritesAsyncReadsWithoutIssues()
        {
            var store = new SecureStore(_store);

            string error = null;
            int numDocs = 0;

            using (Observable.Interval(TimeSpan.FromSeconds(1))
                .SelectMany(async (t, ct) =>
                {
                    if (!ct.IsCancellationRequested)
                    {
                        await _indexer.WriteToIndex(CreateIpsumDocs(200));
                    }
                    return Unit.Default;
                })
                .Subscribe(d => { numDocs++; }, e => { error = e.GetBaseException().Message; }, () => { }))
            {
                for(int i=0; i<5; i++)
                {
                    Task.WaitAll(Enumerable.Range(0, 10).Select(async _ =>
                    {
                        using (var indexer = new LuceneIndex(store, "testindexer", "basePath", null))
                        {
                            await indexer.SearchDocuments(ind =>
                            {
                                var query = new TermQuery(new Term("words", "ipsum"));
                                return ind.Search(query, 20);
                            }).LastOrDefaultAsync();
                        }
                    }).ToArray());
                }
            }

            Assert.AreEqual(null, error);
            Assert.Greater(numDocs, 0);
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
