using Azure;
using Azure.Data.Tables;
using Kalix.Leo.Encryption;
using Kalix.Leo.Table;
using Lokad.Cloud.Storage.Azure;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Table
{
    public class AzureTableQuery<T> : ITableQuery<T>
    {
        private readonly IEnumerable<string> _filter;
        private readonly TableClient _table;
        private readonly IEncryptor _decryptor;
        private readonly int? _take;

        private AzureTableQuery(TableClient table, IEncryptor decryptor, IEnumerable<string> filter, int? take)
        {
            _table = table;
            _decryptor = decryptor;
            _filter = filter;
            _take = take;
        }

        public AzureTableQuery(TableClient table, IEncryptor decryptor)
            : this(table, decryptor, null, null)
        {
        }

        public async Task<T> ById(string partitionKey, string rowKey)
        {
            try
            {
                var entity = await _table.GetEntityAsync<FatEntity>(partitionKey, rowKey);
                return ConvertFatEntity(entity);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                return default;
            }
        }

        public Task<T> FirstOrDefault()
        {
            return ExecuteQuery(_filter, 1).FirstOrDefaultAsync().AsTask();
        }

        public Task<int> Count()
        {
            return ExecuteCount(_filter, CancellationToken.None);
        }

        public ITableQuery<T> PartitionKeyEquals(string partitionKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"PartitionKey eq {partitionKey}"));
        }

        public ITableQuery<T> PartitionKeyLessThan(string partitionKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"PartitionKey lt {partitionKey}"));
        }

        public ITableQuery<T> PartitionKeyLessThanOrEqual(string partitionKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"PartitionKey le {partitionKey}"));
        }

        public ITableQuery<T> PartitionKeyGreaterThan(string partitionKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"PartitionKey gt {partitionKey}"));
        }

        public ITableQuery<T> PartitionKeyGreaterThanOrEqual(string partitionKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"PartitionKey ge {partitionKey}"));
        }

        public ITableQuery<T> PartitionKeyStartsWith(string partitionKey)
        {
            // .startswith is not supported in table queries...
            // instead: we increase the last char by one
            int length = partitionKey.Length;
            char lastChar = partitionKey[length - 1];
            if (lastChar != char.MaxValue)
            {
                lastChar = Convert.ToChar(Convert.ToInt32(lastChar) + 1);
            }
            string endVal = partitionKey[..(length - 1)] + lastChar;

            return NewQuery(TableServiceClient.CreateQueryFilter($"PartitionKey ge {partitionKey} and PartitionKey lt {endVal}"));
        }


        public ITableQuery<T> RowKeyEquals(string rowKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"RowKey eq {rowKey}"));
        }

        public ITableQuery<T> RowKeyLessThan(string rowKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"RowKey lt {rowKey}"));
        }

        public ITableQuery<T> RowKeyLessThanOrEqual(string rowKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"RowKey le {rowKey}"));
        }

        public ITableQuery<T> RowKeyGreaterThan(string rowKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"RowKey gt {rowKey}"));
        }

        public ITableQuery<T> RowKeyGreaterThanOrEqual(string rowKey)
        {
            return NewQuery(TableServiceClient.CreateQueryFilter($"RowKey ge {rowKey}"));
        }

        public ITableQuery<T> RowKeyStartsWith(string rowKey)
        {
            // .startswith is not supported in table queries...
            // instead: we increase the last char by one
            int length = rowKey.Length;
            char lastChar = rowKey[length - 1];
            if (lastChar != char.MaxValue)
            {
                lastChar = Convert.ToChar(Convert.ToInt32(lastChar) + 1);
            }
            string endVal = rowKey[..(length - 1)] + lastChar;

            return NewQuery(TableServiceClient.CreateQueryFilter($"RowKey ge {rowKey} and RowKey lt {endVal}"));
        }

        public IAsyncEnumerable<T> AsEnumerable()
        {
            return ExecuteQuery(_filter, _take);
        }

        private ITableQuery<T> NewQuery(string newFilter)
        {
            var newItem = new[] { newFilter };
            var allFilters = _filter == null ? newItem : _filter.Concat(newItem).ToArray();
            return new AzureTableQuery<T>(_table, _decryptor, allFilters, _take);
        }

        private async IAsyncEnumerable<T> ExecuteQuery(IEnumerable<string> filters, int? take, [EnumeratorCancellation] CancellationToken token = default)
        {
            var filter = $"({string.Join(") and (", filters)})";
            var result = _table.QueryAsync<FatEntity>(filter: filter, maxPerPage: take, cancellationToken: token);
            await foreach (var page in result.AsPages())
            {
                foreach (var item in page.Values)
                {
                    yield return ConvertFatEntity(item);
                }
            }
        }

        // Just select one column to reduce the payload significantly
        private static readonly string[] SelectColumns = new[] { "PartitionKey" };
        private async Task<int> ExecuteCount(IEnumerable<string> filters, CancellationToken token)
        {
            var filter = filters == null || !filters.Any() ? null : $"({string.Join(") and (", filters)})";
            var result = _table.QueryAsync<TableEntity>(filter: filter, select: SelectColumns, cancellationToken: token);
            var count = 0;
            await foreach (var page in result.AsPages())
            {
                count += page.Values.Count;
            }
            return count;
        }

        private T ConvertFatEntity(FatEntity fat)
        {
            T result;
            var data = fat.GetData();

            if (data.Length == 0)
            {
                result = default;
            }
            else
            {
                if (_decryptor != null)
                {
                    using var ms = new MemoryStream();
                    using (var dc = _decryptor.Decrypt(ms, false))
                    {
                        dc.Write(data, 0, data.Length);
                    }

                    data = ms.ToArray();
                }

                result = JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(data));
            }

            return result;
        }
    }
}
