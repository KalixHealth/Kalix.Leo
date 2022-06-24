namespace Kalix.Leo;

/// <summary>
/// Metadata keys that are used by the Leo engine
/// </summary>
public static class MetadataConstants
{
    /// <summary>
    /// Stores the compression algorithm used on the record
    /// </summary>
    public static readonly string CompressionMetadataKey = "leocompression";

    /// <summary>
    /// Stores the encryption algorithm used on the record
    /// </summary>
    public static readonly string EncryptionMetadataKey = "leoencryption";

    /// <summary>
    /// Stores the data type that this record is
    /// </summary>
    public static readonly string TypeMetadataKey = "leotype";

    /// <summary>
    /// Stores the last modified date of the record
    /// </summary>
    public static readonly string ModifiedMetadataKey = "leomodified";

    /// <summary>
    /// Stores the size of the record
    /// </summary>
    public static readonly string ContentLengthMetadataKey = "leocontentlength";

    /// <summary>
    /// Stores the content type of the record
    /// </summary>
    public static readonly string ContentTypeMetadataKey = "leocontenttype2";

    /// <summary>
    /// Stores the audit information
    /// </summary>
    public static readonly string AuditMetadataKey = "leoaudit";

    /// <summary>
    /// We use this metadata to indicate that we want to reindex this method (only useful during index phase)
    /// </summary>
    public static readonly string ReindexMetadataKey = "leoreindex";
}