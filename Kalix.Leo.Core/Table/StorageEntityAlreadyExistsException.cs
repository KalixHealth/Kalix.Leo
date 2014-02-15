using System;

namespace Kalix.Leo.Table
{
    public class StorageEntityAlreadyExistsException : Exception
    {
        public StorageEntityAlreadyExistsException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
