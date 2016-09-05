using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Table
{
    internal static class AzureTableExceptionExtensions
    {
        public static async Task<T> ExecuteWrap<T>(this CloudTable table, Func<CloudTable, Task<T>> method)
        {
            try
            {
                return await method(table).ConfigureAwait(false);
            }
            catch (StorageException e)
            {
                throw e.Wrap("Table: " + table.Name);
            }
        }
    }
}
