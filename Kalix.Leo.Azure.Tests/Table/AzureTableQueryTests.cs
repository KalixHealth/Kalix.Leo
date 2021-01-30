using Kalix.Leo.Azure.Table;
using Lokad.Cloud.Storage.Azure;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using NUnit.Framework;
using System.Text;

namespace Kalix.Leo.Azure.Tests.Table
{
    [TestFixture]
    public class AzureTableQueryTests
    {
        protected CloudTable _table;
        protected AzureTableQuery<TestEntity> _query;

        [SetUp]
        public virtual void Init()
        {
            var client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudTableClient();
            _table = client.GetTableReference("kalixleotablequery");
            _table.CreateIfNotExists();

            _table.Execute(TableOperation.InsertOrReplace(BuildEntity("test1", "test1")));
            _table.Execute(TableOperation.InsertOrReplace(BuildEntity("test1", "test2")));
            _table.Execute(TableOperation.InsertOrReplace(BuildEntity("test2", "test1")));
            _table.Execute(TableOperation.InsertOrReplace(BuildEntity("test2", "test2")));

            _query = new AzureTableQuery<TestEntity>(_table, null);
        }

        [TearDown]
        public virtual void TearDown()
        {
            _table.DeleteIfExists();
        }

        [TestFixture]
        public class CountMethod : AzureTableQueryTests
        {
            [Test]
            public void CountsCorrectly()
            {
                var count = _query.Count().Result;
                Assert.AreEqual(4, count);
            }

            [Test]
            public void CountWithFilterCorrectly()
            {
                var count = _query.PartitionKeyEquals("test1").Count().Result;
                Assert.AreEqual(2, count);
            }
        }

        private FatEntity BuildEntity(string partitionKey, string rowKey)
        {
            var e = new FatEntity { PartitionKey = partitionKey, RowKey = rowKey, ETag = "*" };
            var data = new { testdata = "blah" };
            var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
            e.SetData(bytes, bytes.Length);
            return e;
        }
    }
}
