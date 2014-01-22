using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Queue
{
    public interface IQueueMessage : IDisposable
    {
        string Message { get; }

        Task Complete();
    }
}
