using Kalix.Leo.Storage;
using Kalix.Leo.Table;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Kalix.Leo.Indexing.Config
{
    public class RecordSearchConfig<TMain> : IRecordSearchConfig<TMain>
    {
        private readonly string _tableName;
        private readonly ITableClient _client;

        public RecordSearchConfig(ITableClient client, string tableName)
        {
            _tableName = tableName;
            _client = client;
        }

        public IRecordSearchConfig<TMain, TSearch> WithType<TSearch>(Func<TMain, Metadata, TSearch> searchObjectMap)
            where TSearch : class, new()
        {
            return new RecordSearchConfig<TMain, TSearch>(_client, _tableName, searchObjectMap);
        }
    }

    public class RecordSearchConfig<TMain, TSearch> : IRecordSearchConfig<TMain, TSearch>
    {
        private readonly ITableClient _client;
        private readonly string _tableName;
        private readonly Func<TMain, Metadata, TSearch> _searchObjectMap;
        private readonly List<IRecordMappingConfig<TMain>> _mappings;
        private readonly List<object> _searchandIndexObjects;

        public RecordSearchConfig(ITableClient client, string tableName, Func<TMain, Metadata, TSearch> searchObjectMap)
        {
            _tableName = tableName;
            _searchObjectMap = searchObjectMap;
            _searchandIndexObjects = new List<object>();
            _mappings = new List<IRecordMappingConfig<TMain>>();
            _client = client;
        }

        public IRecordSearchConfig<TMain, TSearch> WithSearch(IRecordSearch search, Func<IRecordSearchMappingStart<TMain, TSearch>, IRecordSearchMappingEnd<TMain, TSearch>> mappings)
        {
            var start = new RecordSearchMappingStart<TMain, TSearch>(search.Prefix, _searchObjectMap, new List<Func<TMain, IEnumerable<object>>>(), null);
            var result = mappings(start) as RecordSearchMappingEnd<TMain, TSearch>;
            _searchandIndexObjects.Add(search);
            _mappings.Add(result);
            return this;
        }

        public IRecordSearchConfig<TMain, TSearch> WithSearch<T1>(IRecordSearch<T1> search, Func<IRecordSearchMappingStart<TMain, TSearch, T1>, IRecordSearchMappingEnd<TMain, TSearch>> mappings, Action<TMain, TSearch, T1> finalMap = null)
        {
            Action<TMain, TSearch, IEnumerable<object>> finalMapAction = (m, s, objs) =>
            {
                if (finalMap != null)
                {
                    T1 obj = objs != null && objs.Count() >= 1 ? (T1)objs.First() : default(T1);
                    finalMap(m, s, obj);
                }
            };
            var start = new RecordSearchMappingStart<TMain, TSearch, T1>(search.Prefix, _searchObjectMap, finalMapAction);
            var result = mappings(start) as RecordSearchMappingEnd<TMain, TSearch>;
            _searchandIndexObjects.Add(search);
            _mappings.Add(result);
            return this;
        }

        public IRecordSearchConfig<TMain, TSearch> WithSearch<T1, T2>(IRecordSearch<T1, T2> search, Func<IRecordSearchMappingStart<TMain, TSearch, T1, T2>, IRecordSearchMappingEnd<TMain, TSearch>> mappings, Action<TMain, TSearch, T1, T2> finalMap = null)
        {
            Action<TMain, TSearch, IEnumerable<object>> finalMapAction = (m, s, objs) =>
            {
                if (finalMap != null)
                {
                    T1 obj = objs != null && objs.Count() >= 1 ? (T1)objs.First() : default(T1);
                    T2 obj2 = objs != null && objs.Count() >= 2 ? (T2)objs.Skip(1).First() : default(T2);
                    finalMap(m, s, obj, obj2);
                }
            };
            var start = new RecordSearchMappingStart<TMain, TSearch, T1, T2>(search.Prefix, _searchObjectMap, finalMapAction);
            var result = mappings(start) as RecordSearchMappingEnd<TMain, TSearch>;
            _searchandIndexObjects.Add(search);
            _mappings.Add(result);
            return this;
        }

        public IRecordSearchConfig<TMain, TSearch> WithUniqueIndex<T1>(IRecordUniqueIndex<T1> index, Func<IRecordIndexMappingStart<TMain, T1>, IRecordIndexMappingEnd<TMain>> mappings)
        {
            var start = new RecordIndexMappingStart<TMain, T1>(index.Prefix);
            var result = mappings(start) as RecordIndexMappingEnd<TMain>;
            _searchandIndexObjects.Add(index);
            _mappings.Add(result);
            return this;
        }

        public IRecordSearchConfig<TMain, TSearch> WithSearch<T1, T2, T3>(IRecordSearch<T1, T2, T3> search, Func<IRecordSearchMappingStart<TMain, TSearch, T1, T2, T3>, IRecordSearchMappingEnd<TMain, TSearch>> mappings, Action<TMain, TSearch, T1, T2, T3> finalMap = null)
        {
            Action<TMain, TSearch, IEnumerable<object>> finalMapAction = (m, s, objs) =>
            {
                if (finalMap != null)
                {
                    T1 obj = objs != null && objs.Count() >= 1 ? (T1)objs.First() : default(T1);
                    T2 obj2 = objs != null && objs.Count() >= 2 ? (T2)objs.Skip(1).First() : default(T2);
                    T3 obj3 = objs != null && objs.Count() >= 3 ? (T3)objs.Skip(2).First() : default(T3);
                    finalMap(m, s, obj, obj2, obj3);
                }
            };
            var start = new RecordSearchMappingStart<TMain, TSearch, T1, T2, T3>(search.Prefix, _searchObjectMap, finalMapAction);
            var result = mappings(start) as RecordSearchMappingEnd<TMain, TSearch>;
            _searchandIndexObjects.Add(search);
            _mappings.Add(result);
            return this;
        }

        public IRecordSearchComposition<TMain, TSearch> Create()
        {
            return new RecordSearchComposition<TMain, TSearch>(_client, _tableName, _mappings, _searchandIndexObjects);
        }
    }

    public class RecordSearchMappingStart<TMain, TSearch> : IRecordSearchMappingStart<TMain, TSearch>
    {
        private readonly Func<TMain, Metadata, TSearch> _searchObjectMap;
        private readonly string _prefix;
        private readonly IEnumerable<Func<TMain, IEnumerable<object>>> _keyMappings;
        private readonly Action<TMain, TSearch, IEnumerable<object>> _finalMapAction;

        public RecordSearchMappingStart(string prefix, Func<TMain, Metadata, TSearch> searchObjectMap, IEnumerable<Func<TMain, IEnumerable<object>>> keyMappings, Action<TMain, TSearch, IEnumerable<object>> finalMapAction)
        {
            _searchObjectMap = searchObjectMap;
            _prefix = prefix;
            _keyMappings = keyMappings;
            _finalMapAction = finalMapAction;
        }

        public IRecordSearchMappingEnd<TMain, TSearch> AlwaysCreate()
        {
            return new RecordSearchMappingEnd<TMain, TSearch>(_prefix, _searchObjectMap, _keyMappings.ToList(), _finalMapAction);
        }

        public IRecordSearchMappingEnd<TMain, TSearch> ConditionallyCreate(Func<TMain, bool> predicate)
        {
            return new RecordSearchMappingEnd<TMain, TSearch>(_prefix, (t, m) =>
            {
                if (predicate(t))
                {
                    return _searchObjectMap(t, m);
                }
                else
                {
                    return default(TSearch);
                }
            }, _keyMappings.ToList(), _finalMapAction);
        }
    }

    public class RecordSearchMappingStart<TMain, TSearch, T1> : IRecordSearchMappingStart<TMain, TSearch, T1>
    {
        private readonly string _prefix;
        private readonly Func<TMain, Metadata, TSearch> _searchObjectMap;
        private readonly Action<TMain, TSearch, IEnumerable<object>> _finalMapAction;

        public RecordSearchMappingStart(string prefix, Func<TMain, Metadata, TSearch> searchObjectMap, Action<TMain, TSearch, IEnumerable<object>> finalMapAction)
        {
            _prefix = prefix;
            _searchObjectMap = searchObjectMap;
            _finalMapAction = finalMapAction;
        }

        public IRecordSearchMappingStart<TMain, TSearch> Arg(Func<TMain, T1> mapping)
        {
            return new RecordSearchMappingStart<TMain, TSearch>(_prefix, _searchObjectMap, new List<Func<TMain, IEnumerable<object>>> { t => new object[] { mapping(t) } }, _finalMapAction);
        }


        public IRecordSearchMappingStart<TMain, TSearch> Arg(Func<TMain, IEnumerable<T1>> mapping)
        {
            return new RecordSearchMappingStart<TMain, TSearch>(_prefix, _searchObjectMap, new List<Func<TMain, IEnumerable<object>>> { t => mapping(t).Cast<object>() }, _finalMapAction);
        }
    }

    public class RecordSearchLastMappingStart<TMain, TSearch, T1> : IRecordSearchLastMappingStart<TMain, TSearch, T1>
    {
        private readonly string _prefix;
        private readonly Func<TMain, Metadata, TSearch> _searchObjectMap;
        private readonly List<Func<TMain, IEnumerable<object>>> _keyMappings;
        private readonly Action<TMain, TSearch, IEnumerable<object>> _finalMapAction;

        public RecordSearchLastMappingStart(string prefix, Func<TMain, Metadata, TSearch> searchObjectMap, List<Func<TMain, IEnumerable<object>>> keyMappings, Action<TMain, TSearch, IEnumerable<object>> finalMapAction)
        {
            _prefix = prefix;
            _searchObjectMap = searchObjectMap;
            _keyMappings = keyMappings;
            _finalMapAction = finalMapAction;
        }

        public IRecordSearchMappingStart<TMain, TSearch> Last(Func<TMain, T1> mapping)
        {
            _keyMappings.Add(m => new object[] { mapping(m) });
            return new RecordSearchMappingStart<TMain, TSearch>(_prefix, _searchObjectMap, _keyMappings, _finalMapAction);
        }

        public IRecordSearchMappingStart<TMain, TSearch> Last(Func<TMain, IEnumerable<T1>> mapping)
        {
            _keyMappings.Add(m => mapping(m).Cast<object>());
            return new RecordSearchMappingStart<TMain, TSearch>(_prefix, _searchObjectMap, _keyMappings, _finalMapAction);
        }
    }

    public class RecordSearchMappingStart<TMain, TSearch, T1, T2> : IRecordSearchMappingStart<TMain, TSearch, T1, T2>
    {
        private readonly string _prefix;
        private readonly Func<TMain, Metadata, TSearch> _searchObjectMap;
        private readonly Action<TMain, TSearch, IEnumerable<object>> _finalMapAction;

        public RecordSearchMappingStart(string prefix, Func<TMain, Metadata, TSearch> searchObjectMap, Action<TMain, TSearch, IEnumerable<object>> finalMapAction)
        {
            _prefix = prefix;
            _searchObjectMap = searchObjectMap;
            _finalMapAction = finalMapAction;
        }

        public IRecordSearchLastMappingStart<TMain, TSearch, T2> First(Func<TMain, T1> mapping)
        {
            return new RecordSearchLastMappingStart<TMain, TSearch, T2>(_prefix, _searchObjectMap, new List<Func<TMain, IEnumerable<object>>> { m => new object[] { mapping(m) } }, _finalMapAction);
        }

        public IRecordSearchLastMappingStart<TMain, TSearch, T2> First(Func<TMain, IEnumerable<T1>> mapping)
        {
            return new RecordSearchLastMappingStart<TMain, TSearch, T2>(_prefix, _searchObjectMap, new List<Func<TMain, IEnumerable<object>>> { m => mapping(m).Cast<object>() }, _finalMapAction);
        }
    }

    public class RecordSearchSecondMappingStart<TMain, TSearch, T1, T2> : IRecordSearchSecondMappingStart<TMain, TSearch, T1, T2>
    {
        private readonly string _prefix;
        private readonly Func<TMain, Metadata, TSearch> _searchObjectMap;
        private readonly List<Func<TMain, IEnumerable<object>>> _keyMappings;
        private readonly Action<TMain, TSearch, IEnumerable<object>> _finalMapAction;

        public RecordSearchSecondMappingStart(string prefix, Func<TMain, Metadata, TSearch> searchObjectMap, List<Func<TMain, IEnumerable<object>>> keyMappings, Action<TMain, TSearch, IEnumerable<object>> finalMapAction)
        {
            _prefix = prefix;
            _searchObjectMap = searchObjectMap;
            _keyMappings = keyMappings;
            _finalMapAction = finalMapAction;
        }

        public IRecordSearchLastMappingStart<TMain, TSearch, T2> Second(Func<TMain, T1> mapping)
        {
            _keyMappings.Add(m => new object[] { mapping(m) });
            return new RecordSearchLastMappingStart<TMain, TSearch, T2>(_prefix, _searchObjectMap, _keyMappings, _finalMapAction);
        }

        public IRecordSearchLastMappingStart<TMain, TSearch, T2> Second(Func<TMain, IEnumerable<T1>> mapping)
        {
            _keyMappings.Add(m => mapping(m).Cast<object>());
            return new RecordSearchLastMappingStart<TMain, TSearch, T2>(_prefix, _searchObjectMap, _keyMappings, _finalMapAction);
        }
    }

    public class RecordSearchMappingStart<TMain, TSearch, T1, T2, T3> : IRecordSearchMappingStart<TMain, TSearch, T1, T2, T3>
    {
        private readonly string _prefix;
        private readonly Func<TMain, Metadata, TSearch> _searchObjectMap;
        private readonly Action<TMain, TSearch, IEnumerable<object>> _finalMapAction;

        public RecordSearchMappingStart(string prefix, Func<TMain, Metadata, TSearch> searchObjectMap, Action<TMain, TSearch, IEnumerable<object>> finalMapAction)
        {
            _prefix = prefix;
            _searchObjectMap = searchObjectMap;
            _finalMapAction = finalMapAction;
        }

        public IRecordSearchSecondMappingStart<TMain, TSearch, T2, T3> First(Func<TMain, T1> mapping)
        {
            return new RecordSearchSecondMappingStart<TMain, TSearch, T2, T3>(_prefix, _searchObjectMap, new List<Func<TMain, IEnumerable<object>>> { m => new object[] { mapping(m) } }, _finalMapAction);
        }

        public IRecordSearchSecondMappingStart<TMain, TSearch, T2, T3> First(Func<TMain, IEnumerable<T1>> mapping)
        {
            return new RecordSearchSecondMappingStart<TMain, TSearch, T2, T3>(_prefix, _searchObjectMap, new List<Func<TMain, IEnumerable<object>>> { m => mapping(m).Cast<object>() }, _finalMapAction);
        }
    }

    public class RecordSearchMappingEnd<TMain, TSearch> : IRecordSearchMappingEnd<TMain, TSearch>, IRecordMappingConfig<TMain>
    {
        private const string Underscore = "_";
        private readonly Func<TMain, Metadata, TSearch> _createFunc;
        private readonly string _prefix;
        private readonly List<Func<TMain, IEnumerable<object>>> _keyMappings;
        private readonly List<Action<TSearch, TSearch>> _newAndOldActions;
        private readonly Action<TMain, TSearch, IEnumerable<object>> _finalMapAction;

        private bool _addIdToRowKey = true;

        public RecordSearchMappingEnd(string prefix, Func<TMain, Metadata, TSearch> createFunc, List<Func<TMain, IEnumerable<object>>> keyMappings, Action<TMain, TSearch, IEnumerable<object>> finalMapAction)
        {
            _createFunc = createFunc;
            _prefix = prefix;
            _keyMappings = keyMappings;
            _newAndOldActions = new List<Action<TSearch, TSearch>>();
            _finalMapAction = finalMapAction;
        }

        public bool IsStrict
        {
            get { return false; }
        }

        public IRecordSearchMappingEnd<TMain, TSearch> AddFinalPass(Action<TSearch, TSearch> newAndOldAction)
        {
            _newAndOldActions.Add(newAndOldAction);
            return this;
        }

        public IRecordSearchMappingEnd<TMain, TSearch> AddIdToRowKey(bool value)
        {
            _addIdToRowKey = value;
            return this;
        }

        public IEnumerable<Action<ITableEntity, ITableEntity>> AdditionalActions { get { return _newAndOldActions.Select(ConvertActionFunc).ToList(); } }

        public IEnumerable<ITableEntity> Create(long partitionKey, string id, ObjectWithMetadata<TMain> model, Func<object, string> keyParser)
        {
            if (_keyMappings.Any())
            {
                var search = _createFunc(model.Data, model.Metadata);
                if (search != null)
                {
                    var rowKeys = FindAllKeys(0, id, model.Data, keyParser, new List<string>());
                    foreach (var key in rowKeys)
                    {
                        search = _createFunc(model.Data, model.Metadata); // Create a new model per item
                        if (_finalMapAction != null)
                        {
                            _finalMapAction(model.Data, search, key.Item2);
                        }
                        yield return new TableEntity<TSearch>(partitionKey.ToString(CultureInfo.InvariantCulture), key.Item1, search);
                    }
                }
            }
            else
            {
                var search = _createFunc(model.Data, model.Metadata);
                if (search != null)
                {
                    if (_finalMapAction != null)
                    {
                        _finalMapAction(model.Data, search, null);
                    }
                    yield return new TableEntity<TSearch>(partitionKey.ToString(CultureInfo.InvariantCulture), GetRowKey(id, null, null), search);
                }
            }
        }

        private Action<ITableEntity, ITableEntity> ConvertActionFunc(Action<TSearch, TSearch> original)
        {
            return (n, o) => original((TSearch)n.DataObject, (TSearch)o.DataObject);
        }

        private string GetRowKey(string id, IEnumerable<object> keys, Func<object, string> keyParser)
        {
            id = id.Replace(Underscore, "");
            var joinedKeys = keys != null && keys.Any() ? Underscore + string.Join(Underscore, keys.Select(k => keyParser(k)).ToArray()) : string.Empty;
            return _prefix + joinedKeys + (_addIdToRowKey ? Underscore + id : string.Empty);
        }

        private IEnumerable<Tuple<string, IEnumerable<object>>> FindAllKeys(int currentMapping, string id, TMain model, Func<object, string> keyParser, IEnumerable<object> currentKeys)
        {
            var mapping = _keyMappings[currentMapping];
            var keys = mapping(model);
            currentMapping++;
            foreach (var key in keys)
            {
                var list = currentKeys.ToList();
                list.Add(key);
                if (currentMapping >= _keyMappings.Count)
                {
                    yield return Tuple.Create(GetRowKey(id, list, keyParser), list.AsEnumerable());
                }
                else
                {
                    foreach (var rowKey in FindAllKeys(currentMapping, id, model, keyParser, list))
                    {
                        yield return rowKey;
                    }
                }
            }
        }
    }

    public class RecordIndexMappingStart<TMain, T1> : IRecordIndexMappingStart<TMain, T1>
    {
        private readonly string _prefix;

        public RecordIndexMappingStart(string prefix)
        {
            _prefix = prefix;
        }

        public IRecordIndexMappingStart<TMain> Arg(Func<TMain, T1> keyMapping)
        {
            return new RecordIndexMappingStart<TMain>(_prefix, new List<Func<TMain, IEnumerable<object>>> { t => new object[] { keyMapping(t) } });
        }

        public IRecordIndexMappingStart<TMain> Arg(Func<TMain, IEnumerable<T1>> keyMapping)
        {
            return new RecordIndexMappingStart<TMain>(_prefix, new List<Func<TMain, IEnumerable<object>>> { t => keyMapping(t).Cast<object>() });
        }
    }

    public class RecordIndexMappingStart<TMain> : IRecordIndexMappingStart<TMain>
    {
        private readonly string _prefix;
        private readonly IEnumerable<Func<TMain, IEnumerable<object>>> _keyMappings;

        public RecordIndexMappingStart(string prefix, IEnumerable<Func<TMain, IEnumerable<object>>> keyMappings)
        {
            _prefix = prefix;
            _keyMappings = keyMappings;
        }

        public IRecordIndexMappingEnd<TMain> AlwaysCreate()
        {
            return new RecordIndexMappingEnd<TMain>(_prefix, _keyMappings.ToList(), t => true);
        }

        public IRecordIndexMappingEnd<TMain> ConditionallyCreate(Func<TMain, bool> predicate)
        {
            return new RecordIndexMappingEnd<TMain>(_prefix, _keyMappings.ToList(), predicate);
        }
    }

    public class RecordIndexMappingEnd<TMain> : IRecordIndexMappingEnd<TMain>, IRecordMappingConfig<TMain>
    {
        private const string Underscore = "_";
        private readonly string _prefix;
        private readonly List<Func<TMain, IEnumerable<object>>> _keyMappings;
        private readonly Func<TMain, bool> _createPredicate;

        public RecordIndexMappingEnd(string prefix, List<Func<TMain, IEnumerable<object>>> keyMappings, Func<TMain, bool> createPredicate)
        {
            _prefix = prefix;
            _keyMappings = keyMappings;
            _createPredicate = createPredicate;
        }

        public bool IsStrict
        {
            get { return true; }
        }

        public IEnumerable<Action<ITableEntity, ITableEntity>> AdditionalActions
        {
            get { return new List<Action<ITableEntity, ITableEntity>>(); }
        }

        public IEnumerable<ITableEntity> Create(long partitionKey, string id, ObjectWithMetadata<TMain> model, Func<object, string> keyParser)
        {
            if (_keyMappings.Any())
            {
                if (_createPredicate(model.Data))
                {
                    var rowKeys = FindAllKeys(0, model.Data, keyParser, new List<string>());
                    var partKey = partitionKey.ToString(CultureInfo.InvariantCulture);
                    foreach (var key in rowKeys)
                    {
                        yield return new TableEntity<object>(partKey, key, null);
                    }
                }
            }
        }

        private string GetRowKey(IEnumerable<object> keys, Func<object, string> keyParser)
        {
            var joinedKeys = keys != null && keys.Any() ? Underscore + string.Join(Underscore, keys.Select(k => keyParser(k)).ToArray()) : string.Empty;
            return _prefix + joinedKeys; // Indexes just store keys
        }

        private IEnumerable<string> FindAllKeys(int currentMapping, TMain model, Func<object, string> keyParser, IEnumerable<object> currentKeys)
        {
            var mapping = _keyMappings[currentMapping];
            var keys = mapping(model);
            currentMapping++;
            foreach (var key in keys)
            {
                var list = currentKeys.ToList();
                list.Add(key);
                if (currentMapping >= _keyMappings.Count)
                {
                    yield return GetRowKey(list, keyParser);
                }
                else
                {
                    foreach (var rowKey in FindAllKeys(currentMapping, model, keyParser, list))
                    {
                        yield return rowKey;
                    }
                }
            }
        }
    }
}
