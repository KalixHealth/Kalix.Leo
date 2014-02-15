using System;

namespace Kalix.Leo.Indexing
{
    public class CompositionException : Exception
    {
        public CompositionException(string rowKey, string message, Exception inner)
            : base(message, inner)
        {
            RowKey = rowKey;
        }

        public string RowKey { get; private set; }
    }
}
