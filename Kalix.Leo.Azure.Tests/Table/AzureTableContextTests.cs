using Kalix.Leo.Azure.Table;
using Lokad.Cloud.Storage.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using NUnit.Framework;
using System.Linq;
using E = Kalix.Leo.Table.ITableEntity;

namespace Kalix.Leo.Azure.Tests.Table
{
    [TestFixture]
    public class AzureTableContextTests
    {
        protected CloudTable _table;
        protected AzureTableContext _azureTable;

        [SetUp]
        public virtual void Init()
        {
            var client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudTableClient();
            _table = client.GetTableReference("kalixleotests");
            _table.CreateIfNotExists();

            _azureTable = new AzureTableContext(_table, null);
        }

        [TearDown]
        public virtual void TearDown()
        {
            _table.DeleteIfExists();
        }

        [TestFixture]
        public class DeleteMethod : AzureTableContextTests
        {
            [Test]
            public void CanDeleteEvenWhenRowDoesNotExist()
            {
                _azureTable.Delete(new TestEntity { RowKey = "test1", PartitionKey = "delete" });
                _azureTable.Delete(new TestEntity { RowKey = "test2", PartitionKey = "delete" });

                _azureTable.Save().Wait();

                var query = new TableQuery<FatEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "delete"));
                var items = _table.ExecuteQuery(query);

                Assert.IsFalse(items.Any());
            }
        }
    }

    public class TestEntity : E
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public object DataObject { get; set; }
    }
}
