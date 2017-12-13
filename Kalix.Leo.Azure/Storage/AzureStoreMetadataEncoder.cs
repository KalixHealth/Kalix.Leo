using System;
using System.Text;

namespace Kalix.Leo.Azure.Storage
{
    /// <summary>
    /// All metadata is encoded into B64 to prevent bad characters
    /// </summary>
    public static class AzureStoreMetadataEncoder
    {
        private const string MetadataPrefix = "B64_";

        /// <summary>
        /// Encodes the metadata str into Base64
        /// </summary>
        /// <param name="original">Original string that may not be safe for metadata</param>
        /// <returns>Base64 string that is safe, with prefix</returns>
        public static string EncodeMetadata(string original)
        {
            if (string.IsNullOrEmpty(original)) { return null; }
            return MetadataPrefix + Convert.ToBase64String(Encoding.UTF8.GetBytes(original));
        }

        /// <summary>
        /// Decodes metadata, if it starts with 'B64_' then it will be assumed to be B64 string
        /// </summary>
        /// <param name="original">Possibly base 64 string, to allow for backwards compatibility</param>
        /// <returns>Unencoded string</returns>
        public static string DecodeMetadata(string original)
        {
            if (original == null) { return null; }

            if (original.StartsWith(MetadataPrefix))
            {
                var data = Convert.FromBase64String(original.Substring(MetadataPrefix.Length));
                return Encoding.UTF8.GetString(data);
            }
            else
            {
                // Support old data
                return original;
            }
        }
    }
}
