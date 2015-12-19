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
        [JsonIgnore]
        public AuditInfo Audit { get; set; }
    }
}
