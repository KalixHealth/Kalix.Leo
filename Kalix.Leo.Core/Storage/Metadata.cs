using System;
using System.Collections.Generic;

namespace Kalix.Leo.Storage
{
    public interface IMetadata : IDictionary<string, string>
    {
        DateTime? LastModified { get; set; }
        long? Size { get; set; }
    }

    public class Metadata : Dictionary<string, string>, IMetadata
    {
        public Metadata() { }
        public Metadata(IDictionary<string, string> initial) : base(initial) { }

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
                    Remove(MetadataConstants.ModifiedMetadataKey);
                }
                else
                {
                    this[MetadataConstants.ModifiedMetadataKey] = value.Value.Ticks.ToString();
                }
            }
        }

        public long? Size
        {
            get
            {
                if (ContainsKey(MetadataConstants.ModifiedMetadataKey))
                {
                    long size;
                    if (long.TryParse(this[MetadataConstants.ModifiedMetadataKey], out size))
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
                    Remove(MetadataConstants.SizeMetadataKey);
                }
                else
                {
                    this[MetadataConstants.SizeMetadataKey] = value.Value.ToString();
                }
            }
        }
    }
}
