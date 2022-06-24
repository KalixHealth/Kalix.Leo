using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Threading.Channels;

namespace System.Collections.Generic;

public static class AsyncEnumerableExtensions
{
    public static async IAsyncEnumerable<bool> Interval(TimeSpan duration, [EnumeratorCancellation] CancellationToken token = default)
    {
        while (!token.IsCancellationRequested)
        {
            await Task.Delay(duration, token);
            yield return true;
        }
    }

    public static async IAsyncEnumerable<T> Combine<T>(this IEnumerable<IAsyncEnumerable<T>> lists, AsyncEnumberableCombineType combineType, int bufferSize, [EnumeratorCancellation] CancellationToken token = default)
    {
        var toCombine = lists.ToArray();
        if (toCombine.Length == 0) { yield break; }

        var opts = new BoundedChannelOptions(bufferSize) { FullMode = BoundedChannelFullMode.Wait };
        var channels = toCombine.Select(_ => Channel.CreateBounded<T>(opts)).ToArray();

        // Tasks that will push the async enumerables to their channel
        for (int i = 0; i < toCombine.Length; i++)
        {
            StartBackgroundCopyTask(toCombine[i], channels[i], token);
        }

        // Our loop to pull items from channels
        int index = 0;
        while (!token.IsCancellationRequested)
        {
            var didRead = false;
            for (var i = index; i < index + channels.Length; i++)
            {
                var j = i % channels.Length;
                if (channels[j].Reader.TryRead(out var item))
                {
                    didRead = true;
                    yield return item;
                    break;
                }
            }

            if (!didRead)
            {
                while (true)
                {
                    // Make sure we have one channel that has data
                    var tasks = channels.Select(c => c.Reader.WaitToReadAsync(token).AsTask()).ToList();
                    var res = await Task.WhenAny(tasks);
                    if (res.Result)
                    {
                        // Channel has data
                        break;
                    }
                    else
                    {
                        // Channel doesn't have data, reduce the channel list
                        var finishedIndex = tasks.IndexOf(res);
                        channels = channels.Where((c, i) => i != finishedIndex).ToArray();
                        if (channels.Length == 0) { yield break; }
                    }
                }
            }

            index = combineType switch
            {
                AsyncEnumberableCombineType.Even => (index + 1) % channels.Length,
                AsyncEnumberableCombineType.FirstHasPriority => 0,
                _ => throw new NotImplementedException("Unknown combine type")
            };
        }
    }

    public static async IAsyncEnumerable<T[]> Buffer<T>(this IAsyncEnumerable<T> enumerable, int buffer, [EnumeratorCancellation] CancellationToken token = default)
    {
        var arr = new List<T>(buffer);
        await foreach (var i in enumerable.WithCancellation(token))
        {
            arr.Add(i);
            if (arr.Count >= buffer)
            {
                yield return arr.ToArray();
                arr.Clear();
            }
        }

        if (arr.Count > 0) { yield return arr.ToArray(); }
    }

    public static async IAsyncEnumerable<T[]> TimedBuffer<T>(this IAsyncEnumerable<T> enumerable, int buffer, TimeSpan maxWaitPerBuffer, [EnumeratorCancellation] CancellationToken token = default)
    {
        var arr = new List<T>(buffer);
        var enumerator = enumerable.GetAsyncEnumerator(token);

        // Every loop will either get items or timeout
        var nextItem = enumerator.MoveNextAsync(token).AsTask();
        var timeout = Task.Delay(maxWaitPerBuffer, token);

        while (!token.IsCancellationRequested)
        {
            var t = await Task.WhenAny(timeout, nextItem);

            if (t == nextItem)
            {
                // We received another item before timeout...
                if (nextItem.Result)
                {
                    // Add the item to buffer
                    arr.Add(enumerator.Current);
                    if (arr.Count >= buffer)
                    {
                        // If we reset the buffer then reset timer as well
                        yield return arr.ToArray();
                        arr.Clear();
                        timeout = Task.Delay(maxWaitPerBuffer, token);
                    }
                    nextItem = enumerator.MoveNextAsync(token).AsTask();
                }
                else
                {
                    // If false then we are done (no more items)
                    if (arr.Count > 0) { yield return arr.ToArray(); }
                    yield break;
                }
            }
            else
            {
                // reached the timeout, only return if we have any items
                if (arr.Count > 0)
                {
                    yield return arr.ToArray();
                    arr.Clear();
                }
                timeout = Task.Delay(maxWaitPerBuffer, token);
            }
        }
    }

    public static IAsyncDisposable TakeUntilDisposed<T>(this IAsyncEnumerable<T> enumerable, CancellationTokenSource src = null, Func<Task> onDispose = null, Action<Exception> onError = null)
    {
        return new AsyncDisposeManager<T>(enumerable, src, onDispose, onError);
    }

    private static void StartBackgroundCopyTask<T>(IAsyncEnumerable<T> copyFrom, Channel<T> copyTo, CancellationToken token)
    {
        _ = Task.Run(() => copyFrom.ForEachAwaitAsync(async t => await copyTo.Writer.WriteAsync(t, token), token), token)
                .ContinueWith(t => copyTo.Writer.Complete(t.Exception), token);
    }

    private sealed class AsyncDisposeManager<T> : IAsyncDisposable
    {
        private readonly CancellationTokenSource _tcs;
        private readonly Task _task;
        private readonly Func<Task> _onDispose;
        private readonly Action<Exception> _onError;

        public AsyncDisposeManager(IAsyncEnumerable<T> enumerable, CancellationTokenSource src, Func<Task> onDispose, Action<Exception> onError)
        {
            _tcs = src ?? new CancellationTokenSource();
            _task = ExecuteTask(enumerable);
            _onDispose = onDispose;
            _onError = onError;
        }

        private async Task ExecuteTask(IAsyncEnumerable<T> enumerable)
        {
            try
            {
                await foreach (var _ in enumerable.WithCancellation(_tcs.Token)) { }
            }
            catch (Exception e)
            {
                _onError?.Invoke(e);
            }
        }

        public async ValueTask DisposeAsync()
        {
            _tcs.Cancel();
            try { await _task; } catch { }
            _tcs.Dispose();
            if (_onDispose != null)
            {
                await _onDispose();
            }
        }
    }
}

public enum AsyncEnumberableCombineType
{
    FirstHasPriority = 0,
    Even
}