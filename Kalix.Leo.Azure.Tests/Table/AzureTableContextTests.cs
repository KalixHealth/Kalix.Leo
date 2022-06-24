using Azure.Data.Tables;
using Kalix.Leo.Azure.Table;
using Lokad.Cloud.Storage.Azure;
using NUnit.Framework;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests.Table;

[TestFixture]
public class AzureTableContextTests
{
    protected TableClient _table;
    protected AzureTableContext _azureTable;

    [SetUp]
    public virtual async Task Init()
    {
        _table = AzureTestsHelper.GetTable("kalixleotablecontext");
        await _table.CreateIfNotExistsAsync();

        _azureTable = new AzureTableContext(_table, null);
    }

    [TearDown]
    public virtual Task TearDown()
    {
        return _table.DeleteAsync();
    }

    [TestFixture]
    public class DeleteMethod : AzureTableContextTests
    {
        [Test]
        public async Task CanDeleteEvenWhenRowDoesNotExist()
        {
            _azureTable.Delete(new TestEntity { RowKey = "test1", PartitionKey = "delete" });
            _azureTable.Delete(new TestEntity { RowKey = "test2", PartitionKey = "delete" });

            _azureTable.Save().Wait();

            var items = await _table.QueryAsync<FatEntity>(filter: "PartitionKey eq 'delete'").ToListAsync();
            Assert.IsFalse(items.Any());
        }
    }
}