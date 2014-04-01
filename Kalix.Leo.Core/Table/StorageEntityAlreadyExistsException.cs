using System;

namespace Kalix.Leo.Table
{
    /// <summary>
    /// Exception used when there is a conflict of an index
    /// </summary>
    public class StorageEntityAlreadyExistsException : Exception
    {
        /// <summary>
        /// Constructor 
        /// </summary>
        public StorageEntityAlreadyExistsException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
