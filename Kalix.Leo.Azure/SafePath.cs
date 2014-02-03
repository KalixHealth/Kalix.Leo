
namespace Kalix.Leo.Azure
{
    public static class SafePath
    {
        public static string MakeSafeFilePath(string str)
        {
            return (str ?? string.Empty).Replace('\\', '/');
        }
    }
}
