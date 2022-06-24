namespace Kalix.Leo.Storage;

public sealed class ObjectWithMetadata<T>
{
    private readonly Metadata _metadata;
    private readonly T _data;

    public ObjectWithMetadata(T data, Metadata metadata = null)
    {
        _metadata = metadata ?? new Metadata();
        _data = data;
    }

    public Metadata Metadata { get { return _metadata; } }
    public T Data { get { return _data; } }
}