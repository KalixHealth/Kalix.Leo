using Microsoft.WindowsAzure.Storage;
using System.Linq;
using System.Net;

namespace Kalix.Leo.Azure
{
    internal static class StorageExceptionExtensions
    {
        // http://alexandrebrisebois.wordpress.com/2013/07/03/handling-windows-azure-storage-exceptions/
        public static AzureException Wrap(this StorageException ex, string path)
        {
            var requestInformation = ex.RequestInformation;
            var information = requestInformation.ExtendedErrorInformation;

            // if you have aditional information, you can use it for your logs
            if (information == null)
            {
                return new AzureException(string.Format("An unknown azure exception occurred for path '{0}': {1}", path, ex.Message), ex);
            }
            
            var message = string.Format("({0}) {1} - '{2}'", information.ErrorCode, information.ErrorMessage, path);

            var details = information
                .AdditionalDetails
                .Aggregate(string.Empty, (s, pair) =>
                {
                    return s + string.Format("{0}={1},", pair.Key, pair.Value);
                })
                .Trim(',');

            return new AzureException(message + " details " + details, ex);
        }
    }
}
