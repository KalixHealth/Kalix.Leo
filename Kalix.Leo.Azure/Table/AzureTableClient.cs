using Azure.Data.Tables;
using Kalix.Leo.Encryption;
using Kalix.Leo.Table;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Table
{
    public sealed class AzureTableClient : ITableClient
    {
        private readonly string _tableNamePrefix;
        private readonly TableServiceClient _client;

        public AzureTableClient(TableServiceClient client, string tableNamePrefix = null)
        {
            _client = client;
            _tableNamePrefix = tableNamePrefix ?? "Leo";
        }

        public Task CreateTableIfNotExist(string tableName)
        {
            var table = _client.GetTableClient(GetName(tableName));
            return table.CreateIfNotExistsAsync();
        }

        public Task DeleteTableIfExists(string tableName)
        {
            var table = _client.GetTableClient(GetName(tableName));
            return table.DeleteAsync();
        }

        public ITableContext Context(string tableName, IEncryptor encryptor)
        {
            return new AzureTableContext(_client.GetTableClient(GetName(tableName)), encryptor);
        }

        public ITableQuery<T> Query<T>(string tableName, IEncryptor decryptor)
        {
            return new AzureTableQuery<T>(_client.GetTableClient(GetName(tableName)), decryptor);
        }

        public string GetName(string tableName)
        {
            return _tableNamePrefix + tableName;
        }
    }
}
