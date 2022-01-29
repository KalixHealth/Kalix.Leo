using Azure;
using Azure.Data.Tables;
using Kalix.Leo.Azure.Table;
using Lokad.Cloud.Storage.Azure;
using Newtonsoft.Json;
using NUnit.Framework;
using System.Text;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests.Table
{
    [TestFixture]
    public class AzureTableQueryTests
    {
        protected TableClient _table;
        protected AzureTableQuery<TestEntity> _query;

        [SetUp]
        public virtual async Task Init()
        {
            _table = AzureTestsHelper.GetTable("kalixleotablequery");
            await _table.CreateIfNotExistsAsync();

            await _table.UpsertEntityAsync(BuildEntity("test1", "test1"), TableUpdateMode.Replace);
            await _table.UpsertEntityAsync(BuildEntity("test1", "test2"), TableUpdateMode.Replace);
            await _table.UpsertEntityAsync(BuildEntity("test2", "test1"), TableUpdateMode.Replace);
            await _table.UpsertEntityAsync(BuildEntity("test2", "test2"), TableUpdateMode.Replace);

            _query = new AzureTableQuery<TestEntity>(_table, null);
        }

        [TearDown]
        public virtual async Task TearDown()
        {
            await _table.DeleteAsync();
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

            [Test]
            public void EmptyResultsOk()
            {
                var count = _query.PartitionKeyEquals("test5").Count().Result;
                Assert.AreEqual(0, count);
            }
        }

        private static FatEntity BuildEntity(string partitionKey, string rowKey)
        {
            var e = new FatEntity { PartitionKey = partitionKey, RowKey = rowKey, ETag = ETag.All };
            var data = new { testdata = "blah" };
            var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
            e.SetData(bytes, bytes.Length);
            return e;
        }
    }
}
