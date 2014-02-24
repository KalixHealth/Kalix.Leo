using System;
using System.Globalization;
using System.IO;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace Kalix.Leo.Storage
{
    /// <summary>
    /// Used to generate simple, unique identifiers across multiple environments, processes and/or threads. Requires a global data
    /// store that can be used to store the last upper limit (must implement the IOptimisticSyncStore interface). Contention is reduced
    /// by allocating ranges to each instance of the UniqueIdGenerator. The RangeSize should increase proportionally with the fre
    /// </summary>
    public sealed class UniqueIdGenerator : IUniqueIdGenerator
    {
        private readonly int _maxRetries;
        private readonly IOptimisticStore _store;
        private readonly StoreLocation _location;
        private readonly int _rangeSize;
        private readonly object _taskLock;

        private Task _innerUpdateTask;
        private long _internalId;
        private long _upperIdLimit;

        public UniqueIdGenerator(
            IOptimisticStore store,
            StoreLocation location,
            int rangeSize = 10,
            int maxRetries = 25)
        {
            _taskLock = new object();
            _rangeSize = rangeSize;
            _maxRetries = maxRetries;
            _store = store;
            _location = location;
        }

        /// <summary>
        /// Fetches the next available unique ID
        /// </summary>
        /// <returns></returns>
        public Task<long> NextId()
        {
            var current = Interlocked.Increment(ref _internalId);
            if (current >= _upperIdLimit)
            {
                // Only need to lock this piece of code to make sure that
                // 1) There is only ever one task running at any one time
                // 2) That the value for _innerUpdateTask always has a value for
                //    'ContinueWith' on the next line
                lock(_taskLock)
                { 
                    // If inner task is done then reset!
                    if (_innerUpdateTask != null && _innerUpdateTask.IsCompleted)
                    {
                        _innerUpdateTask = null;
                    }

                    _innerUpdateTask = _innerUpdateTask ?? UpdateFromSyncStore();
                }

                // Try the method again
                return _innerUpdateTask.ContinueWith(t => NextId()).Unwrap();
            }
            else
            {
                return Task.FromResult(current);
            }
        }

        public async Task SetCurrentId(long newId)
        {
            if(newId <= 0)
            {
                throw new ArgumentException("newId must be greater than 0", "newId");
            }

            var limitBytes = Encoding.UTF8.GetBytes(newId.ToString(CultureInfo.InvariantCulture));
            var limitData = new DataWithMetadata(Observable.Return(limitBytes));
            if (await _store.TryOptimisticWrite(_location, limitData))
            {
                // This will force a refresh on the Next
                _upperIdLimit = 0;
            }
            else
            {
                throw new InvalidOperationException("Could not update the id");
            }
        }

        private async Task UpdateFromSyncStore()
        {
            int retryCount = 0;

            // maxRetries + 1 because the first run isn't a 're'try.
            while (retryCount < _maxRetries + 1)
            {
                string data = null;

                var dataStream = await _store.LoadData(_location);
                if (dataStream != null)
                {
                    data = await dataStream.Stream
                        .ToList()
                        .Select(b => 
                        {
                            var all = b.SelectMany(a => a).ToArray();
                            return Encoding.UTF8.GetString(all, 0, all.Length);
                        });
                }

                long currentId;
                if (data == null)
                {
                    currentId = 0;
                }
                else
                {
                    if (!long.TryParse(data, out currentId))
                    {
                        throw new Exception(string.Format("Data '{0}' in storage was corrupt and could not be parsed as a long", data));
                    }
                    if(currentId < 0)
                    {
                        throw new Exception(string.Format("Saved Id cannot be less than 0"));
                    }
                }

                var upperLimit = currentId + _rangeSize;

                var limitBytes = Encoding.UTF8.GetBytes(upperLimit.ToString(CultureInfo.InvariantCulture));
                var limitData = new DataWithMetadata(Observable.Return(limitBytes));
                if (await _store.TryOptimisticWrite(_location, limitData))
                {
                    // First update currentId
                    // Then upper limit, this will avoid any need for locks etc
                    _internalId = currentId;
                    _upperIdLimit = upperLimit;

                    return;
                }

                retryCount++;
                // update failed, go back around the loop
            }

            throw new Exception(string.Format("Failed to update the OptimisticSyncStore after {0} attempts", retryCount));
        }
    }
}
