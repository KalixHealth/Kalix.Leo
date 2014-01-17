using Kalix.Leo.Azure.Storage;
using Kalix.Leo.Storage;
using Lucene.Net.Documents;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using System.Diagnostics;
using System.Reactive.Linq;

namespace Kalix.Leo.Lucene.Tests
{
    [TestFixture]
    public class IndexerTests
    {
        protected IOptimisticStore _store;
        protected Indexer _indexer;

        [SetUp]
        public void Init()
        {
            var client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient();
            _store = new AzureStore(client, false, null, true); // Do not snapshot and do tracing!
            _store.CreateContainerIfNotExists("testindexer");

            var store = new SecureStore(_store);
            _indexer = new Indexer(store, "testindexer");
        }

        [TearDown]
        public void TearDown()
        {
            _indexer.Dispose();
            _store.PermanentDeleteContainer("testindexer");
        }

        [Test]
        public void Write100000EntriesNoErrors()
        {
            var docs = Observable.Range(0, 100000)
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

            _indexer.WriteToIndex(docs).Wait();
        }
    }
}
