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
                if(ContainsKey(MetadataConstants.ModifiedMetadataKey))
                {
                    long ticks;
                    if(long.TryParse(this[MetadataConstants.ModifiedMetadataKey], out ticks))
                    {
                        val = new DateTime(ticks, DateTimeKind.Utc);
                    }
                }
                return val;
            }
            set
            {
                if(value.HasValue)
                {
                    this[MetadataConstants.ModifiedMetadataKey] = value.Value.Ticks.ToString(CultureInfo.InvariantCulture);
                }
                else
                {
                    Remove(MetadataConstants.ModifiedMetadataKey);
                }
            }
        }

        /// <summary>
        /// Common metadata key, holds the size of the specified record
        /// </summary>
        public long? Size
        {
            get
            {
                if (ContainsKey(MetadataConstants.SizeMetadataKey))
                {
                    long size;
                    if (long.TryParse(this[MetadataConstants.SizeMetadataKey], out size))
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
                    this[MetadataConstants.SizeMetadataKey] = value.Value.ToString(CultureInfo.InvariantCulture);
                }
                else
                {
                    Remove(MetadataConstants.SizeMetadataKey);
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
                else
                {
                    Remove(MetadataConstants.ContentTypeMetadataKey);
                }
            }
        }

        /// <summary>
        /// A tag to indicate the current version of the metadata, only valid when loading from storage
        /// </summary>
        public string ETag { get; set; }
    }
}
