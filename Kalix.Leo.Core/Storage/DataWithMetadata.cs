using System;
using System.Collections.Generic;

namespace Kalix.Leo.Storage
{
    public sealed class DataWithMetadata
    {
        private readonly IMetadata _metadata;
        private readonly IObservable<byte[]> _stream;

        public DataWithMetadata(IObservable<byte[]> stream, IMetadata metadata = null)
        {
            _metadata = metadata ?? new Metadata();
            _stream = stream;
        }

        public IMetadata Metadata { get { return _metadata; } }
        public IObservable<byte[]> Stream { get { return _stream; } }
    }
}
