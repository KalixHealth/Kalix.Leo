using Kalix.Leo.Storage;
using System;

namespace Kalix.Leo
{
    public sealed class DataWithMetadata : IDisposable
    {
        private readonly Metadata _metadata;
        private readonly IObservable<byte[]> _stream;
        private readonly Action _onDispose;
        private bool _isDisposed;

        public DataWithMetadata(IObservable<byte[]> stream, Metadata metadata = null, Action onDispose = null)
        {
            _metadata = metadata ?? new Metadata();
            _stream = stream;
            _onDispose = onDispose;
        }

        public Metadata Metadata { get { return _metadata; } }
        public IObservable<byte[]> Stream { get { return _stream; } }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                if (_onDispose != null)
                {
                    _onDispose();
                }

                _isDisposed = true;
            }
        }
    }
}
