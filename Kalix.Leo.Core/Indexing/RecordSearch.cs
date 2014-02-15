using System;
using System.Collections.Generic;
using System.Linq;

namespace Kalix.Leo.Indexing
{
    public class RecordSearch : IRecordSearch
    {
        private readonly string _prefix;

        public RecordSearch(string prefix)
        {
            _prefix = prefix;
        }

        public string Prefix { get { return _prefix; } }
        
        public IRecordSearch<DateTime> AddDateArg()
        {
            return new RecordSearch<DateTime>(_prefix);
        }

        public IRecordSearch<long> AddLongArg()
        {
            return new RecordSearch<long>(_prefix);
        }
    }

    public class RecordSearch<T1> : IRecordSearch<T1>
    {
        private readonly string _prefix;

        public RecordSearch(string prefix)
        {
            _prefix = prefix;
        }

        public string Prefix { get { return _prefix; } }

        public IRecordSearch<T1, DateTime> AddDateArg()
        {
            return new RecordSearch<T1, DateTime>(_prefix);
        }

        public IRecordSearch<T1, long> AddLongArg()
        {
            return new RecordSearch<T1, long>(_prefix);
        }
    }

    public class RecordSearch<T1, T2> : IRecordSearch<T1, T2>
    {
        private readonly string _prefix;

        public RecordSearch(string prefix)
        {
            _prefix = prefix;
        }

        public string Prefix { get { return _prefix; } }

        public IRecordSearch<T1, T2, DateTime> AddDateArg()
        {
            return new RecordSearch<T1, T2, DateTime>(_prefix);
        }

        public IRecordSearch<T1, T2, long> AddLongArg()
        {
            return new RecordSearch<T1, T2, long>(_prefix);
        }
    }

    public class RecordSearch<T1, T2, T3> : IRecordSearch<T1, T2, T3>
    {
        private readonly string _prefix;

        public RecordSearch(string prefix)
        {
            _prefix = prefix;
        }

        public string Prefix { get { return _prefix; } }
    }
}