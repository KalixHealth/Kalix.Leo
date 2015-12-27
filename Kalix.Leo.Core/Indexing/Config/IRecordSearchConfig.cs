using Kalix.Leo.Storage;
using Kalix.Leo.Table;
using System;
using System.Collections.Generic;

namespace Kalix.Leo.Indexing.Config
{
    public interface IRecordSearchConfig<TMain>
    {
        IRecordSearchConfig<TMain, TSearch> WithType<TSearch>(Func<TMain, Metadata, TSearch> searchObjectMap) where TSearch : class, new();
    }

    public interface IRecordSearchConfig<TMain, TSearch>
    {
        IRecordSearchConfig<TMain, TSearch> WithSearch(IRecordSearch search, Func<IRecordSearchMappingStart<TMain, TSearch>, IRecordSearchMappingEnd<TMain, TSearch>> mappings);
        IRecordSearchConfig<TMain, TSearch> WithSearch<T1>(IRecordSearch<T1> search, Func<IRecordSearchMappingStart<TMain, TSearch, T1>, IRecordSearchMappingEnd<TMain, TSearch>> mappings, Action<TMain, TSearch, T1> finalMap = null);
        IRecordSearchConfig<TMain, TSearch> WithSearch<T1, T2>(IRecordSearch<T1, T2> search, Func<IRecordSearchMappingStart<TMain, TSearch, T1, T2>, IRecordSearchMappingEnd<TMain, TSearch>> mappings, Action<TMain, TSearch, T1, T2> finalMap = null);
        IRecordSearchConfig<TMain, TSearch> WithSearch<T1, T2, T3>(IRecordSearch<T1, T2, T3> search, Func<IRecordSearchMappingStart<TMain, TSearch, T1, T2, T3>, IRecordSearchMappingEnd<TMain, TSearch>> mappings, Action<TMain, TSearch, T1, T2, T3> finalMap = null);
        IRecordSearchConfig<TMain, TSearch> WithUniqueIndex<T1>(IRecordUniqueIndex<T1> index, Func<IRecordIndexMappingStart<TMain, T1>, IRecordIndexMappingEnd<TMain>> mappings);
        IRecordSearchComposition<TMain, TSearch> Create();
    }

    public interface IRecordSearchMappingStart<TMain, TSearch>
    {
        IRecordSearchMappingEnd<TMain, TSearch> AlwaysCreate();
        IRecordSearchMappingEnd<TMain, TSearch> ConditionallyCreate(Func<TMain, bool> predicate);
    }

    public interface IRecordSearchMappingStart<TMain, TSearch, T1>
    {
        IRecordSearchMappingStart<TMain, TSearch> Arg(Func<TMain, T1> keyMapping);
        IRecordSearchMappingStart<TMain, TSearch> Arg(Func<TMain, IEnumerable<T1>> keyMapping);
    }

    public interface IRecordSearchLastMappingStart<TMain, TSearch, T1>
    {
        IRecordSearchMappingStart<TMain, TSearch> Last(Func<TMain, T1> keyMapping);
        IRecordSearchMappingStart<TMain, TSearch> Last(Func<TMain, IEnumerable<T1>> keyMapping);
    }

    public interface IRecordSearchMappingStart<TMain, TSearch, T1, T2>
    {
        IRecordSearchLastMappingStart<TMain, TSearch, T2> First(Func<TMain, T1> keyMapping);
        IRecordSearchLastMappingStart<TMain, TSearch, T2> First(Func<TMain, IEnumerable<T1>> keyMapping);
    }

    public interface IRecordSearchSecondMappingStart<TMain, TSearch, T1, T2>
    {
        IRecordSearchLastMappingStart<TMain, TSearch, T2> Second(Func<TMain, T1> keyMapping);
        IRecordSearchLastMappingStart<TMain, TSearch, T2> Second(Func<TMain, IEnumerable<T1>> keyMapping);
    }

    public interface IRecordSearchMappingStart<TMain, TSearch, T1, T2, T3>
    {
        IRecordSearchSecondMappingStart<TMain, TSearch, T2, T3> First(Func<TMain, T1> keyMapping);
        IRecordSearchSecondMappingStart<TMain, TSearch, T2, T3> First(Func<TMain, IEnumerable<T1>> keyMapping);
    }

    public interface IRecordSearchMappingEnd<TMain, TSearch>
    {
        IRecordSearchMappingEnd<TMain, TSearch> AddFinalPass(Action<TSearch, TSearch> newAndOldAction);
        IRecordSearchMappingEnd<TMain, TSearch> AddIdToRowKey(bool value);
    }

    public interface IRecordIndexMappingStart<TMain, T1>
    {
        IRecordIndexMappingStart<TMain> Arg(Func<TMain, T1> keyMapping);
        IRecordIndexMappingStart<TMain> Arg(Func<TMain, IEnumerable<T1>> keyMapping);
    }

    public interface IRecordIndexMappingStart<TMain>
    {
        IRecordIndexMappingEnd<TMain> AlwaysCreate();
        IRecordIndexMappingEnd<TMain> ConditionallyCreate(Func<TMain, bool> predicate);
    }

    public interface IRecordIndexMappingEnd<TMain>
    {
    }

    public interface IRecordMappingConfig<TMain>
    {
        IEnumerable<Action<ITableEntity, ITableEntity>> AdditionalActions { get; }
        IEnumerable<ITableEntity> Create(long partitionKey, string id, ObjectWithMetadata<TMain> model, Func<object, string> keyParser);

        bool IsStrict { get; }
    }
}
