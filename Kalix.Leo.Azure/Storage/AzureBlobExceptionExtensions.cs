using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage
{
    internal static class AzureBlobExceptionExtensions
    {
        public static async Task<T> ExecuteWrap<T>(this CloudBlockBlob blob, Func<CloudBlockBlob, Task<T>> method, bool notFoundOk = false)
        {
            try
            {
                return await method(blob).ConfigureAwait(false);
            }
            catch (StorageException e)
            {
                // No metadata to update in this case...
                if (notFoundOk && e.RequestInformation.HttpStatusCode == 404)
                {
                    return default(T);
                }

                throw e.Wrap($"{blob.Container.Name}/${blob.Name}");
            }
        }

        public static async Task<bool> ExecuteWrap(this CloudBlockBlob blob, Func<CloudBlockBlob, Task> method, bool notFoundOk = false)
        {
            try
            {
                await method(blob).ConfigureAwait(false);
                return true;
            }
            catch (StorageException e)
            {
                // No metadata to update in this case...
                if (notFoundOk && e.RequestInformation.HttpStatusCode == 404)
                {
                    return false;
                }

                throw e.Wrap($"{blob.Container.Name}/${blob.Name}");
            }
        }
    }
}
