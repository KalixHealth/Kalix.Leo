namespace Kalix.Leo;

/// <summary>
/// Simple object to hold an id and some metadata
/// </summary>
public struct IdWithMetadata
{
    private readonly long _id;
    private readonly Metadata _metadata;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="id">Id to store</param>
    /// <param name="metadata">Metadata to store</param>
    public IdWithMetadata(long id, Metadata metadata)
    {
        _id = id;
        _metadata = metadata;
    }

    /// <summary>
    /// Id associated with this record
    /// </summary>
    public long Id { get { return _id; } }
        
    /// <summary>
    /// Metadata associated with this record
    /// </summary>
    public Metadata Metadata { get { return _metadata; } }
}