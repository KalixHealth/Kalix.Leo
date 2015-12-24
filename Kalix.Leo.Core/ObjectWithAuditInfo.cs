using Newtonsoft.Json;

namespace Kalix.Leo
{
    /// <summary>
    /// Base class that should be used with all objects that store data
    /// </summary>
    public class ObjectWithAuditInfo
    {
        /// <summary>
        /// Audit property contains the Audit information you need
        /// Note: this will be set from metadata automatically and so does not get serialized
        /// </summary>
        public AuditInfo Audit { get; set; }

        /// <summary>
        /// Control function whether to serialize the Audit object
        /// </summary>
        /// <returns></returns>
        public bool ShouldSerializeAudit()
        {
            return !HideAuditInfo;
        }

        /// <summary>
        /// Control whether to store the Audit JSON or not
        /// </summary>
        [JsonIgnore]
        public bool HideAuditInfo { get; set; }
    }
}
