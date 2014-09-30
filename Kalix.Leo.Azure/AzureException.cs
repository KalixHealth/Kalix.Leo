using System;

namespace Kalix.Leo.Azure
{
    public class AzureException : Exception
    {
        public AzureException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
