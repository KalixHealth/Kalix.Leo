using System;

namespace Kalix.Leo
{
    /// <summary>
    /// Data stream that also holds metadata
    /// Is generally a 'hot' stream so disposing it will close the underlying stream correctly
    /// </summary>
    public sealed class DataWithMetadata : IDisposable
    {
        private readonly Metadata _metadata;
        private readonly IObservable<byte[]> _stream;
        private readonly Action _onDispose;
        private bool _isDisposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="stream">A stream of data</param>
        /// <param name="metadata">Metadata to include</param>
        /// <param name="onDispose">Any action to take when this object is disposed</param>
        public DataWithMetadata(IObservable<byte[]> stream, Metadata metadata = null, Action onDispose = null)
        {
            _metadata = metadata ?? new Metadata();
            _stream = stream;
            _onDispose = onDispose;
        }

        /// <summary>
        /// Metadata for the associated stream of data
        /// </summary>
        public Metadata Metadata { get { return _metadata; } }

        /// <summary>
        /// The stream of data, assume that it is a 'hot' observable
        /// </summary>
        public IObservable<byte[]> Stream { get { return _stream; } }

        /// <summary>
        /// Dispose
        /// </summary>
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
