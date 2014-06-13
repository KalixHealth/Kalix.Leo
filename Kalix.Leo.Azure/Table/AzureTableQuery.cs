using AsyncBridge;
using Kalix.Leo.Encryption;
using Kalix.Leo.Table;
using Lokad.Cloud.Storage.Azure;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using CT = Microsoft.WindowsAzure.Storage.Table;

namespace Kalix.Leo.Azure.Table
{
    public class AzureTableQuery<T> : ITableQuery<T>
    {
        private const string PartitionKey = "PartitionKey";
        private const string RowKey = "RowKey";

        private readonly string _filter;
        private readonly CT.CloudTable _table;
        private readonly IEncryptor _decryptor;
        private readonly int? _take;

        private AzureTableQuery(CT.CloudTable table, IEncryptor decryptor, string filter, int? take)
        {
            _table = table;
            _decryptor = decryptor;
            _filter = filter;
            _take = take;
        }

        public AzureTableQuery(CT.CloudTable table, IEncryptor decryptor)
            : this(table, decryptor, null, null)
        {
        }

        public async Task<T> ById(string partitionKey, string rowKey)
        {
            CT.TableOperation op = CT.TableOperation.Retrieve<FatEntity>(partitionKey, rowKey);
            CT.TableResult result = await _table.ExecuteAsync(op).ConfigureAwait(false);
            if(result.Result == null)
            {
                return default(T);
            }

            return ConvertFatEntity((FatEntity)result.Result);            
        }

        public async Task<T> FirstOrDefault()
        {
            return await ExecuteQuery(_filter, 1).FirstOrDefaultAsync();
        }

        public ITableQuery<T> PartitionKeyEquals(string partitionKey)
        {
            string newFilter = CT.TableQuery.GenerateFilterCondition(PartitionKey, CT.QueryComparisons.Equal, partitionKey);
            return NewQuery(newFilter);
        }

        public ITableQuery<T> RowKeyEquals(string rowKey)
        {
            string newFilter = CT.TableQuery.GenerateFilterCondition(RowKey, CT.QueryComparisons.Equal, rowKey);
            return NewQuery(newFilter);
        }

        public ITableQuery<T> RowKeyLessThan(string rowKey)
        {
            string newFilter = CT.TableQuery.GenerateFilterCondition(RowKey, CT.QueryComparisons.LessThan, rowKey);
            return NewQuery(newFilter);
        }

        public ITableQuery<T> RowKeyLessThanOrEqual(string rowKey)
        {
            string newFilter = CT.TableQuery.GenerateFilterCondition(RowKey, CT.QueryComparisons.LessThanOrEqual, rowKey);
            return NewQuery(newFilter);
        }

        public ITableQuery<T> RowKeyGreaterThan(string rowKey)
        {
            string newFilter = CT.TableQuery.GenerateFilterCondition(RowKey, CT.QueryComparisons.GreaterThan, rowKey);
            return NewQuery(newFilter);
        }

        public ITableQuery<T> RowKeyGreaterThanOrEqual(string rowKey)
        {
            string newFilter = CT.TableQuery.GenerateFilterCondition(RowKey, CT.QueryComparisons.GreaterThanOrEqual, rowKey);
            return NewQuery(newFilter);
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
            string endVal = rowKey.Substring(0, length - 1) + lastChar;

            string newFilter = CT.TableQuery.CombineFilters(
                CT.TableQuery.GenerateFilterCondition(RowKey, CT.QueryComparisons.GreaterThanOrEqual, rowKey),
                CT.TableOperators.And,
                CT.TableQuery.GenerateFilterCondition(RowKey, CT.QueryComparisons.LessThan, endVal));

            return NewQuery(newFilter);
        }

        public IObservable<T> AsObservable()
        {
            return ExecuteQuery(_filter, _take);
        }

        private ITableQuery<T> NewQuery(string newFilter)
        {
            if (_filter != null)
            {
                newFilter = CT.TableQuery.CombineFilters(_filter, CT.TableOperators.And, newFilter);
            }
            return new AzureTableQuery<T>(_table, _decryptor, newFilter, _take);
        }

        private IObservable<T> ExecuteQuery(string filter, int? take)
        {
            return Observable.Create<T>(async (obs, ct) =>
            {
                var query = new CT.TableQuery<FatEntity>();
                if (filter != null)
                {
                    query = query.Where(filter);
                }
                if (take.HasValue)
                {
                    query = query.Take(take);
                }

                CT.TableQuerySegment<FatEntity> segment = null;
                while((segment == null || segment.ContinuationToken != null) && !ct.IsCancellationRequested)
                {
                    segment = await _table.ExecuteQuerySegmentedAsync(query, segment == null ? null : segment.ContinuationToken, ct).ConfigureAwait(false);
                    foreach(var entity in segment)
                    {
                        obs.OnNext(ConvertFatEntity(entity));
                    }
                }

                obs.OnCompleted();
            }).SubscribeOn(TaskPoolScheduler.Default);
        }

        private T ConvertFatEntity(FatEntity entity)
        {
            T result = default(T);
            using (var w = AsyncHelper.Wait)
            {
                w.Run(ConvertFatEntityAsync(entity), f => result = f);
            }
            return result;
        }

        private async Task<T> ConvertFatEntityAsync(FatEntity fat)
        {
            T result;
            var data = fat.GetData();

            if (data.Length == 0)
            {
                result = default(T);
            }
            else
            {
                if (_decryptor != null)
                {
                    var decrypted = await _decryptor.Decrypt(Observable.Return(data)).ToList();
                    data = new byte[decrypted.Sum(d => d.LongLength)];
                    int offset = 0;
                    foreach (var d in decrypted)
                    {
                        Buffer.BlockCopy(d, 0, data, offset, d.Length);
                        offset += d.Length;
                    }
                }

                result = JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(data));
            }

            return result;
        }
    }
}
