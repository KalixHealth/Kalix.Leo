using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kalix.Leo.Table
{
    public interface ITableQuery<T>
    {
        Task<T> FirstOrDefault();
        Task<T> ById(string partitionKey, string rowKey);

        ITableQuery<T> PartitionKeyEquals(string partitionKey);

        ITableQuery<T> RowKeyEquals(string rowKey);
        ITableQuery<T> RowKeyLessThan(string rowKey);
        ITableQuery<T> RowKeyLessThanOrEqual(string rowKey);
        ITableQuery<T> RowKeyGreaterThan(string rowKey);
        ITableQuery<T> RowKeyGreaterThanOrEqual(string rowKey);
        ITableQuery<T> RowKeyStartsWith(string rowKey);

        IAsyncEnumerable<T> AsEnumerable();
    }
}
