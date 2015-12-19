namespace Kalix.Leo
{
    /// <summary>
    /// The class we use when updating records
    /// </summary>
    public class UpdateAuditInfo
    {
        /// <summary>
        /// Identifier of who updated this record
        /// </summary>
        public string UpdatedBy { get; set; }

        /// <summary>
        /// The name of the person who updated this record
        /// </summary>
        public string UpdatedByName { get; set; }
    }
}
