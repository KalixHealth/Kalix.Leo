using System;

namespace Kalix.Leo;

/// <summary>
/// Audit Info that needs to be stored against every object
/// </summary>
public class AuditInfo
{
    /// <summary>
    /// Identifier of who updated this record
    /// </summary>
    public string UpdatedBy { get; set; }

    /// <summary>
    /// The name of the person who updated this record
    /// </summary>
    public string UpdatedByName { get; set; }

    /// <summary>
    /// Identifier of who created this record
    /// </summary>
    public string CreatedBy { get; set; }

    /// <summary>
    /// The name of the person who created this record
    /// </summary>
    public string CreatedByName { get; set; }

    /// <summary>
    /// The last time this record was updated
    /// </summary>
    public DateTime? UpdatedOn { get; set; }

    /// <summary>
    /// The first time this record was created
    /// </summary>
    public DateTime? CreatedOn { get; set; }

    /// <summary>
    /// Convenience method to convert to update object
    /// </summary>
    /// <returns>Update Audit object</returns>
    public UpdateAuditInfo ToUpdateAuditInfo()
    {
        return new UpdateAuditInfo
        {
            UpdatedBy = UpdatedBy,
            UpdatedByName = UpdatedByName
        };
    }
}