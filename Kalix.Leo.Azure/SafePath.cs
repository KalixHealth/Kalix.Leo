
namespace Kalix.Leo.Azure
{
    /// <summary>
    /// Utility class to normalize file paths
    /// </summary>
    public static class SafePath
    {
        /// <summary>
        /// Makes sure a path is valid for Azure
        /// </summary>
        /// <param name="str">Input path</param>
        /// <returns>Normalized file path</returns>
        public static string MakeSafeFilePath(string str)
        {
            return (str ?? string.Empty).Replace('\\', '/');
        }
    }
}
