/*
    Copyright (c) 2012, iD Commerce + Logistics
    All rights reserved.

    Redistribution and use in source and binary forms, with or without modification, are permitted
    provided that the following conditions are met:

    Redistributions of source code must retain the above copyright notice, this list of conditions
    and the following disclaimer. Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the documentation and/or other
    materials provided with the distribution.
 
    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
    IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
    FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
    CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
    SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
    OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace System.Collections.Generic
{
    /// <summary>
    /// Provides extension methods for IAsyncEnumerable.
    /// </summary>
    public static class AsyncEnumerableExtensions
    {
        /// <summary>
        /// Unwraps an async enumerable of tasks.
        /// </summary>
        /// <typeparam name="T">The type to extract.</typeparam>
        /// <param name="e">The enumerable to unwrap.</param>
        /// <returns>An async enumerable of unwrapped objects.</returns>
        public static IAsyncEnumerable<T> Unwrap<T>(this IAsyncEnumerable<Task<T>> e)
        {
            if (e == null) throw new ArgumentNullException("e");

            return new UnwrapEnumerable<T>(e);
        }

        /// <summary>
        /// Unwraps an async enumerable of tasks.
        /// </summary>
        /// <param name="e">The enumerable to unwrap.</param>
        /// <returns>An async enumerable of unwrapped tasks.</returns>
        public static IAsyncEnumerable<bool> Unwrap(this IAsyncEnumerable<Task> e)
        {
            if (e == null) throw new ArgumentNullException("e");

            return new UnwrapEnumerable<bool>(e.Select(t => t.ContinueWith(ta => true)));
        }

        /// <summary>
        /// Filters elements in an async enumerable based on the result of an async function.
        /// </summary>
        /// <typeparam name="T">The type of item to filter.</typeparam>
        /// <param name="e">The enumerable to filter.</param>
        /// <param name="test">The function to test if items should be kept.</param>
        /// <returns>An async enumerable of filtered objects.</returns>
        public static IAsyncEnumerable<T> Where<T>(this IAsyncEnumerable<T> e, Func<T, Task<bool>> test)
        {
            if (e == null) throw new ArgumentNullException("e");
            if (test == null) throw new ArgumentNullException("test");

            return e.Select(async x => new
            {
                Item = x,
                Keep = await test(x).ConfigureAwait(false)
            }).Unwrap().Where(x => x.Keep).Select(x => x.Item);
        }

        /// <summary>
        /// Filters elements in an async enumerable based on the result of an async function.
        /// </summary>
        /// <typeparam name="T">The type of item to filter.</typeparam>
        /// <param name="e">The enumerable to filter.</param>
        /// <param name="test">The function to test if items should be kept.</param>
        /// <returns>An async enumerable of filtered objects.</returns>
        public static IAsyncEnumerable<T> Where<T>(this IAsyncEnumerable<T> e, Func<T, int, Task<bool>> test)
        {
            if (e == null) throw new ArgumentNullException("e");
            if (test == null) throw new ArgumentNullException("test");

            return e.Select(async (x, i) => new
            {
                Item = x,
                Keep = await test(x, i).ConfigureAwait(false)
            }).Unwrap().Where(x => x.Keep).Select(x => x.Item);
        }

        public static EnumerableCanceller<T> TakeUntilDisposed<T>(this IAsyncEnumerable<T> e, TimeSpan? maxWaitTime, Action<Task> onFinish)
        {
            return new EnumerableCanceller<T>(e, maxWaitTime, onFinish);
        }

        private sealed class UnwrapEnumerable<T> : IAsyncEnumerable<T>
        {
            readonly IAsyncEnumerable<Task<T>> src;

            public UnwrapEnumerable(IAsyncEnumerable<Task<T>> src)
            {
                Debug.Assert(src != null);

                this.src = src;
            }

            public IAsyncEnumerator<T> GetEnumerator()
            {
                return new UnwrapEnumerator<T>(src);
            }
        }

        private sealed class UnwrapEnumerator<T> : IAsyncEnumerator<T>
        {
            IAsyncEnumerator<Task<T>> src;
            T current;

            public T Current { get { return current; } }

            public UnwrapEnumerator(IAsyncEnumerable<Task<T>> src)
            {
                Debug.Assert(src != null);

                this.src = src.GetEnumerator();
            }

            public async Task<bool> MoveNext(CancellationToken cancellationToken)
            {
                if (src == null)
                {
                    throw new ObjectDisposedException("UnwrapEnumerator");
                }

                if (await src.MoveNext(cancellationToken).ConfigureAwait(false))
                {
                    current = await src.Current.ConfigureAwait(false);
                    return true;
                }

                return false;
            }

            public void Dispose()
            {
                if (src != null)
                {
                    src.Dispose();
                    src = null;
                }
            }
        }
    }

    public sealed class EnumerableCanceller<T> : IDisposable
    {
        private readonly IAsyncEnumerator<T> _enumerator;
        private readonly CancellationTokenSource _cts;
        private readonly CancellationToken _token;

        public EnumerableCanceller(IAsyncEnumerable<T> e, TimeSpan? maxWaitTime, Action<Task> onFinish)
        {
            _enumerator = e.GetEnumerator();
            _cts = maxWaitTime.HasValue ? new CancellationTokenSource(maxWaitTime.Value) : new CancellationTokenSource();
            _token = _cts.Token;

            RunningTask = WaitLoop().ContinueWith(t =>
            {
                onFinish(t);
                return t;
            }).Unwrap();
        }

        public Task RunningTask { get; private set; }

        private async Task WaitLoop()
        {
            if (!await _enumerator.MoveNext(_token).ConfigureAwait(false))
            {
                _cts.Token.ThrowIfCancellationRequested();
                return;
            }

            _cts.Token.ThrowIfCancellationRequested();
            await WaitLoop().ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (!RunningTask.IsCompleted)
            {
                _cts.Cancel();

                try
                {
                    RunningTask.Wait();
                }
                catch (Exception) { }
            }

            _cts.Dispose();
            _enumerator.Dispose();
        }
    }
}