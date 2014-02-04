using System;
using System.Collections.Generic;

namespace Kalix.Leo
{
    public class Metadata : Dictionary<string, string>
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
                    this[MetadataConstants.ModifiedMetadataKey] = value.Value.Ticks.ToString();
                }
                else
                {
                    Remove(MetadataConstants.ModifiedMetadataKey);
                }
            }
        }

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
                    this[MetadataConstants.SizeMetadataKey] = value.Value.ToString();
                }
                else
                {
                    Remove(MetadataConstants.SizeMetadataKey);
                }
            }
        }
    }
}
