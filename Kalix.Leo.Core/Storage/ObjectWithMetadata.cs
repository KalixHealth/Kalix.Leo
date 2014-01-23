namespace Kalix.Leo.Storage
{
    public sealed class ObjectWithMetadata<T>
    {
        private readonly IMetadata _metadata;
        private readonly T _data;

        public ObjectWithMetadata(T data, IMetadata metadata = null)
        {
            _metadata = metadata ?? new Metadata();
            _data = data;
        }

        public IMetadata Metadata { get { return _metadata; } }
        public T Data { get { return _data; } }
    }
}
