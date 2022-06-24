using Kalix.Leo.Azure.Table;
using NUnit.Framework;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests.Table;

[TestFixture]
public class AzureTableClientTests
{
    protected AzureTableClient _client;

    [SetUp]
    public virtual void Init()
    {
        _client = new AzureTableClient(AzureTestsHelper.GetTableService());
    }

    [TestFixture]
    public class DeleteTableIfExistsMethod : AzureTableClientTests
    {
        [Test]
        public Task CanDeleteEvenIfTableDoesntExist()
        {
            return _client.DeleteTableIfExists("doesnotexist");
        }
    }
}