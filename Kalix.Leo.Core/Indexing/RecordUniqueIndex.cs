namespace Kalix.Leo.Indexing;

public class RecordUniqueIndex<T> : IRecordUniqueIndex<T>
{
    private readonly string _prefix;

    public RecordUniqueIndex(string prefix)
    {
        _prefix = prefix;
    }

    public string Prefix
    {
        get { return _prefix; }
    }
}