using Microsoft.WindowsAzure.Storage;
using System.Linq;

namespace Kalix.Leo.Azure
{
    public static class StorageExceptionExtensions
    {
        // http://alexandrebrisebois.wordpress.com/2013/07/03/handling-windows-azure-storage-exceptions/
        public static AzureException Wrap(this StorageException ex)
        {
            var requestInformation = ex.RequestInformation;
            var information = requestInformation.ExtendedErrorInformation;

            // if you have aditional information, you can use it for your logs
            if (information == null)
            {
                return new AzureException("An unknown azure exception occurred: " + ex.Message, ex);
            }

            var errorCode = information.ErrorCode;

            var message = string.Format("({0}) {1}",
                errorCode,
                information.ErrorMessage);

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
