using System.Threading.Tasks;

namespace System
{
    public static class AsyncDisposable
    {
        public static IAsyncDisposable Create(Func<ValueTask> func)
        {
            return new AsyncDisposableInner(func);
        }

        private sealed class AsyncDisposableInner : IAsyncDisposable
        {
            private readonly Func<ValueTask> _func;

            public AsyncDisposableInner(Func<ValueTask> func)
            {
                _func = func;
            }

            public ValueTask DisposeAsync()
            {
                return _func();
            }
        }
    }
}
