using System;

namespace Kalix.Leo.Indexing;

public interface IRecordSearch
{
    string Prefix { get; }
    IRecordSearch<DateTime> AddDateArg();
    IRecordSearch<long> AddLongArg();
}

public interface IRecordSearch<T1>
{
    string Prefix { get; }
    IRecordSearch<T1, DateTime> AddDateArg();
    IRecordSearch<T1, long> AddLongArg();
}

public interface IRecordSearch<T1, T2>
{
    string Prefix { get; }
    IRecordSearch<T1, T2, DateTime> AddDateArg();
    IRecordSearch<T1, T2, long> AddLongArg();
}

public interface IRecordSearch<T1, T2, T3>
{
    string Prefix { get; }
}