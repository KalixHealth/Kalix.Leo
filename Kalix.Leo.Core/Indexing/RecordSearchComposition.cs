﻿using Kalix.Leo.Encryption;
using Kalix.Leo.Indexing.Config;
using Kalix.Leo.Storage;
using Kalix.Leo.Table;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Indexing
{
    public class RecordSearchComposition<TMain, TSearch> : IRecordSearchComposition<TMain, TSearch>
    {
        private const char Underscore = '_';
        private readonly string _tableName;
        private readonly ITableClient _client;
        private readonly IEnumerable<IRecordMappingConfig<TMain>> _mappings;
        private readonly IEnumerable<object> _validSearches;

        public RecordSearchComposition(ITableClient client, 
            string tableName,
            IEnumerable<IRecordMappingConfig<TMain>> mappings, IEnumerable<object> validSearches)
        {
            _tableName = tableName;
            _mappings = mappings;
            _client = client;
            _validSearches = validSearches;

            client.CreateTableIfNotExist(tableName);
        }

        public async Task Save(long partitionKey, string id, ObjectWithMetadata<TMain> item, ObjectWithMetadata<TMain> previous, IEncryptor encryptor)
        {
            var context = _client.Context(_tableName, encryptor);

            var rowKeys = new List<string>();
            foreach (var mapping in _mappings)
            {
                var oldItems = new List<ITableEntity>();
                if (previous != null)
                {
                    oldItems = mapping.Create(partitionKey, id, previous, KeyParser).ToList();
                }

                var newItems = mapping.Create(partitionKey, id, item, KeyParser).ToList();

                // Remove any old items that are not in the new items collection
                foreach(var oldItem in oldItems.Where(o => !newItems.Any(n => n.RowKey == o.RowKey)))
                {
                    context.Delete(oldItem);
                    rowKeys.Add(oldItem.RowKey);
                }

                // If we have any new items lets update/create them
                foreach(var newItem in newItems)
                {
                    var matchingOld = oldItems.FirstOrDefault(o => o.RowKey == newItem.RowKey);

                    if (matchingOld != null && mapping.AdditionalActions.Any())
                    {
                        // Get the real old item to map from
                        var actualOldItem = await _client.Query<TSearch>(_tableName, encryptor).ById(matchingOld.PartitionKey, matchingOld.RowKey).ConfigureAwait(false);
                        var actualOldItemEntity = new TableEntity<TSearch>(matchingOld.PartitionKey, matchingOld.RowKey, actualOldItem);

                        foreach (var action in mapping.AdditionalActions)
                        {
                            action(newItem, actualOldItemEntity);
                        }
                    }

                    // Be specific about which update we want...
                    rowKeys.Add(newItem.RowKey);
                    if (mapping.IsStrict && matchingOld == null)
                    {
                        // If there is no old item for this set, then expect this to be actually a new item
                        context.Insert(newItem);
                    }
                    else
                    {
                        context.InsertOrReplace(newItem);
                    }
                }
            }

            try
            {
                await context.Save().ConfigureAwait(false);
            }
            catch (StorageEntityAlreadyExistsException e)
            {
                var failedItem = e.Message.Split(':')[0];
                int failedEntity;
                string rowKey = string.Empty;
                if (int.TryParse(failedItem, out failedEntity) && failedEntity < rowKeys.Count)
                {
                    rowKey = rowKeys[failedEntity];
                }
                throw new CompositionException(rowKey, "Entity already exists", e);
            }
        }

        public Task Delete(long partitionKey, string id, ObjectWithMetadata<TMain> main)
        {
            var context = _client.Context(_tableName, null);

            var entity = main as ITableEntity;
            if (entity != null)
            {
                context.Delete(entity);
            }

            // Remove all related entities
            foreach (var mapping in _mappings)
            {
                var items = mapping.Create(partitionKey, id, main, KeyParser).ToList();
                foreach (var item in items)
                {
                    context.Delete(item);
                }
            }

            return context.Save();
        }

        public IAsyncEnumerable<TSearch> SearchAll(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch search)
        {
            return Search(partitionKey, encryptor, search.Prefix, search);
        }

        public IAsyncEnumerable<TSearch> SearchAll<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1> search)
        {
            return Search(partitionKey, encryptor, search.Prefix, search);
        }

        public IAsyncEnumerable<TSearch> SearchAll<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search)
        {
            return Search(partitionKey, encryptor, search.Prefix, search);
        }

        private IAsyncEnumerable<TSearch> Search(long partitionKey, Lazy<Task<IEncryptor>> encryptor, string prefix, object search)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            return ExecuteWithEncryptor(encryptor, e => _client.Query<TSearch>(_tableName, e)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyStartsWith(prefix + Underscore)
                .AsEnumerable());
        }

        public IAsyncEnumerable<TSearch> SearchFor<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search, T1 val)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            return ExecuteWithEncryptor(encryptor, e => _client.Query<TSearch>(_tableName, e)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyStartsWith(search.Prefix + Underscore + KeyParser(val) + Underscore)
                .AsEnumerable());
        }

        public IAsyncEnumerable<TSearch> SearchFor<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search, T1 val, T2 val2)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            return ExecuteWithEncryptor(encryptor, e => _client.Query<TSearch>(_tableName, e)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyStartsWith(search.Prefix + Underscore + KeyParser(val) + Underscore + KeyParser(val2) + Underscore)
                .AsEnumerable());
        }

        public IAsyncEnumerable<TSearch> SearchFor<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1> search, T1 val)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            return ExecuteWithEncryptor(encryptor, e => _client.Query<TSearch>(_tableName, e)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyStartsWith(search.Prefix + Underscore + KeyParser(val) + Underscore)
                .AsEnumerable());
        }

        public IAsyncEnumerable<TSearch> SearchBetween<T1>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1> search, T1 start, T1 end)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            string actualStart = KeyParser(start);
            string actualEnd = KeyParser(end);
            if (actualEnd.CompareTo(actualStart) < 0)
            {
                var temp = actualStart;
                actualStart = actualEnd;
                actualEnd = temp;
            }

            // End value is inclusive, lets add a char to the underscore so it includes everything
            return ExecuteWithEncryptor(encryptor, e => _client.Query<TSearch>(_tableName, e)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyGreaterThan(search.Prefix + Underscore + actualStart + Underscore)
                .RowKeyLessThan(search.Prefix + Underscore + actualEnd + Convert.ToChar(Convert.ToInt32(Underscore) + 1))
                .AsEnumerable());
        }

        public IAsyncEnumerable<TSearch> SearchBetween<T1, T2>(long partitionKey, Lazy<Task<IEncryptor>> encryptor, IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            string actualVal = KeyParser(val);
            string actualStart = KeyParser(start);
            string actualEnd = KeyParser(end);
            if (actualEnd.CompareTo(actualStart) < 0)
            {
                var temp = actualStart;
                actualStart = actualEnd;
                actualEnd = temp;
            }

            // End value is inclusive, lets add a char to the underscore so it includes everything
            return ExecuteWithEncryptor(encryptor, e => _client.Query<TSearch>(_tableName, e)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyGreaterThan(search.Prefix + Underscore + actualVal + Underscore + actualStart + Underscore)
                .RowKeyLessThan(search.Prefix + Underscore + actualVal + Underscore + actualEnd + Convert.ToChar(Convert.ToInt32(Underscore) + 1))
                .AsEnumerable());
        }

        public Task<int> CountAll(long partitionKey, IRecordSearch search)
        {
            return Count(partitionKey, search.Prefix, search);
        }

        public Task<int> CountAll<T1>(long partitionKey, IRecordSearch<T1> search)
        {
            return Count(partitionKey, search.Prefix, search);
        }

        public Task<int> CountAll<T1, T2>(long partitionKey, IRecordSearch<T1, T2> search)
        {
            return Count(partitionKey, search.Prefix, search);
        }

        private Task<int> Count(long partitionKey, string prefix, object search)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            return _client.Query<TSearch>(_tableName, null)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyStartsWith(prefix + Underscore)
                .Count();
        }

        public Task<int> CountFor<T1, T2>(long partitionKey, IRecordSearch<T1, T2> search, T1 val)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            return _client.Query<TSearch>(_tableName, null)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyStartsWith(search.Prefix + Underscore + KeyParser(val) + Underscore)
                .Count();
        }

        public Task<int> CountFor<T1, T2>(long partitionKey, IRecordSearch<T1, T2> search, T1 val, T2 val2)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            return _client.Query<TSearch>(_tableName, null)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyStartsWith(search.Prefix + Underscore + KeyParser(val) + Underscore + KeyParser(val2) + Underscore)
                .Count();
        }

        public Task<int> CountFor<T1>(long partitionKey, IRecordSearch<T1> search, T1 val)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            return _client.Query<TSearch>(_tableName, null)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyStartsWith(search.Prefix + Underscore + KeyParser(val) + Underscore)
                .Count();
        }

        public Task<int> CountBetween<T1>(long partitionKey, IRecordSearch<T1> search, T1 start, T1 end)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            string actualStart = KeyParser(start);
            string actualEnd = KeyParser(end);
            if (actualEnd.CompareTo(actualStart) < 0)
            {
                var temp = actualStart;
                actualStart = actualEnd;
                actualEnd = temp;
            }

            // End value is inclusive, lets add a char to the underscore so it includes everything
            return _client.Query<TSearch>(_tableName, null)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyGreaterThan(search.Prefix + Underscore + actualStart + Underscore)
                .RowKeyLessThan(search.Prefix + Underscore + actualEnd + Convert.ToChar(Convert.ToInt32(Underscore) + 1))
                .Count();
        }

        public Task<int> CountBetween<T1, T2>(long partitionKey, IRecordSearch<T1, T2> search, T1 val, T2 start, T2 end)
        {
            if (!_validSearches.Any(v => v.Equals(search)))
            {
                throw new InvalidOperationException("This search has not been added as a mapping");
            }

            string actualVal = KeyParser(val);
            string actualStart = KeyParser(start);
            string actualEnd = KeyParser(end);
            if (actualEnd.CompareTo(actualStart) < 0)
            {
                var temp = actualStart;
                actualStart = actualEnd;
                actualEnd = temp;
            }

            // End value is inclusive, lets add a char to the underscore so it includes everything
            return _client.Query<TSearch>(_tableName, null)
                .PartitionKeyEquals(partitionKey.ToString(CultureInfo.InvariantCulture))
                .RowKeyGreaterThan(search.Prefix + Underscore + actualVal + Underscore + actualStart + Underscore)
                .RowKeyLessThan(search.Prefix + Underscore + actualVal + Underscore + actualEnd + Convert.ToChar(Convert.ToInt32(Underscore) + 1))
                .Count();
        }

        private IAsyncEnumerable<T> ExecuteWithEncryptor<T>(Lazy<Task<IEncryptor>> encryptor, Func<IEncryptor, IAsyncEnumerable<T>> factory)
        {
            encryptor = encryptor ?? new Lazy<Task<IEncryptor>>(() => Task.FromResult((IEncryptor)null));

            return AsyncEnumerableEx.Create((Func<AsyncYielder<T>, Task>)(async y =>
            {
                var enc = await encryptor.Value.ConfigureAwait(false);
                var enumerator = factory(enc).GetEnumerator();
                while(!y.CancellationToken.IsCancellationRequested)
                {
                    if(!await enumerator.MoveNext(y.CancellationToken).ConfigureAwait(false))
                    {
                        break;
                    }
                    await y.YieldReturn(enumerator.Current).ConfigureAwait(false);
                }
            }));
        }

        private string KeyParser(object key)
        {
            if (key is long)
            {
                return ((long)key).ToString(CultureInfo.InvariantCulture);
            }

            if (key is DateTime)
            {
                return InverseTicksString((DateTime)key);
            }

            if (key is string)
            {
                return (string)key;
            }

            throw new InvalidOperationException("Key type " + key.GetType().Name + " is unkown");
        }

        private string InverseTicksString(DateTime date)
        {
            if (date.Kind == DateTimeKind.Local)
            {
                throw new InvalidOperationException("Datetime is local");
            }

            return (DateTime.MaxValue - date).Ticks.ToString(CultureInfo.InvariantCulture);
        }
    }
}