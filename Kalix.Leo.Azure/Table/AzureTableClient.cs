using Kalix.Leo.Encryption;
using Kalix.Leo.Table;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Table
{
    public sealed class AzureTableClient : ITableClient
    {
        private readonly string _tableNamePrefix;
        private readonly CloudTableClient _client;

        public AzureTableClient(CloudTableClient client, string tableNamePrefix = null)
        {
            _client = client;
            _tableNamePrefix = tableNamePrefix ?? "Leo";
        }

        public Task CreateTableIfNotExist(string tableName)
        {
            return _client.GetTableReference(GetName(tableName)).ExecuteWrap(t => t.CreateIfNotExistsAsync());
        }

        public Task DeleteTableIfExists(string tableName)
        {
            return _client.GetTableReference(GetName(tableName)).ExecuteWrap(t => t.DeleteIfExistsAsync());
        }

        public ITableContext Context(string tableName, IEncryptor encryptor)
        {
            return new AzureTableContext(_client.GetTableReference(GetName(tableName)), encryptor);
        }

        public ITableQuery<T> Query<T>(string tableName, IEncryptor decryptor)
        {
            return new AzureTableQuery<T>(_client.GetTableReference(GetName(tableName)), decryptor);
        }

        public string GetName(string tableName)
        {
            return _tableNamePrefix + tableName;
        }
    }
}
