using Kalix.Leo.Encryption;
using System.Threading.Tasks;

namespace Kalix.Leo.Table
{
    public interface ITableClient
    {
        Task CreateTableIfNotExist(string tableName);
        Task DeleteTableIfExists(string tableName);

        ITableContext Context(string tableName, IEncryptor encryptor);
        ITableQuery<T> Query<T>(string tableName, IEncryptor decryptor);
    }
}
