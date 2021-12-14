using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace Kalix.Leo
{
    /// <summary>
    /// Metadata class where you can add arbritrary data to a record
    /// </summary>
    public class Metadata : Dictionary<string, string>
    {
        private readonly static JsonSerializerSettings JsonSettings = new()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            ContractResolver = new CamelCasePropertyNamesContractResolver()
        };

        /// <summary>
        /// Simple constructor
        /// </summary>
        public Metadata() { }

        /// <summary>
        /// Simple constructor to create metadata with a single key
        /// </summary>
        /// <param name="key">Key to add</param>
        /// <param name="value">Value to add for the specified key</param>
        public Metadata(string key, string value) : base(new Dictionary<string, string> { { key, value } }) { }

        /// <summary>
        /// Simple constructor to add a number of initial metadata keys
        /// </summary>
        /// <param name="initial">Dictionary of values</param>
        public Metadata(IDictionary<string, string> initial) : base(initial ?? new Dictionary<string, string>()) { }

        /// <summary>
        /// Common metadata key, holds the date that a record was modified
        /// </summary>
        public DateTime? LastModified
        {
            get
            {
                DateTime? val = null;
                if (ContainsKey(MetadataConstants.ModifiedMetadataKey))
                {
                    if (long.TryParse(this[MetadataConstants.ModifiedMetadataKey], out long ticks))
                    {
                        val = new DateTime(ticks, DateTimeKind.Utc);
                    }
                }
                return val;
            }
            set
            {
                if (value.HasValue)
                {
                    this[MetadataConstants.ModifiedMetadataKey] = value.Value.Ticks.ToString(CultureInfo.InvariantCulture);
                }
                else if (ContainsKey(MetadataConstants.ModifiedMetadataKey))
                {
                    Remove(MetadataConstants.ModifiedMetadataKey);
                }
            }
        }

        /// <summary>
        /// Common metadata key, holds the size of the specified record
        /// </summary>
        public long? ContentLength
        {
            get
            {
                if (ContainsKey(MetadataConstants.ContentLengthMetadataKey))
                {
                    if (long.TryParse(this[MetadataConstants.ContentLengthMetadataKey], out long size))
                    {
                        return size;
                    }
                }
                return null;
            }
            set
            {
                if (value.HasValue)
                {
                    this[MetadataConstants.ContentLengthMetadataKey] = value.Value.ToString(CultureInfo.InvariantCulture);
                }
                else if (ContainsKey(MetadataConstants.ContentLengthMetadataKey))
                {
                    Remove(MetadataConstants.ContentLengthMetadataKey);
                }
            }
        }

        /// <summary>
        /// Common metadata key, holds the content type of the record
        /// </summary>
        public string ContentType
        {
            get
            {
                if (ContainsKey(MetadataConstants.ContentTypeMetadataKey))
                {
                    return this[MetadataConstants.ContentTypeMetadataKey];
                }
                return null;
            }
            set
            {
                if (!string.IsNullOrWhiteSpace(value))
                {
                    this[MetadataConstants.ContentTypeMetadataKey] = value;
                }
                else if (ContainsKey(MetadataConstants.ContentTypeMetadataKey))
                {
                    Remove(MetadataConstants.ContentTypeMetadataKey);
                }
            }
        }

        /// <summary>
        /// A tag to indicate the current version of the metadata, only valid when loading from storage
        /// </summary>
        public string ETag { get; set; }

        /// <summary>
        /// Snapshot id of the current data, should always have a value when loading from storage
        /// </summary>
        public string Snapshot { get; set; }

        /// <summary>
        /// The size of the file as stored via the storage provider, will generally be bigger than Size due to encryption etc
        /// </summary>
        public long? StoredContentLength { get; set; }

        /// <summary>
        /// The Content-Type of the file as stored in the storage provider, will generally be binary due to encryption
        /// </summary>
        public string StoredContentType { get; set; }

        /// <summary>
        /// Holds the date/time that a record was modified from the underlying storage provider
        /// </summary>
        public DateTime? StoredLastModified { get; set; }

        /// <summary>
        /// Holds the audit information, generally this is automatically populated
        /// </summary>
        public AuditInfo Audit
        {
            get
            {
                if (ContainsKey(MetadataConstants.AuditMetadataKey))
                {
                    var str = this[MetadataConstants.AuditMetadataKey];
                    return string.IsNullOrWhiteSpace(str) ? new AuditInfo() : JsonConvert.DeserializeObject<AuditInfo>(str, JsonSettings);
                }
                return new AuditInfo();
            }
            set
            {
                if (value != null)
                {
                    var str = JsonConvert.SerializeObject(value, JsonSettings); 
                    this[MetadataConstants.AuditMetadataKey] = str;
                }
                else if (ContainsKey(MetadataConstants.AuditMetadataKey))
                {
                    Remove(MetadataConstants.AuditMetadataKey);
                }
            }
        }
    }
}
