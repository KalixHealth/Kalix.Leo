using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public class PollingMessageListener : IDisposable
    {
        private readonly CancellationTokenSource _tokenSource;
        private bool _isDisposed;

        public PollingMessageListener(CloudQueue queue, Func<string, Task> executeMessage)
        {
            _tokenSource = new CancellationTokenSource();

            Task.Factory.StartNew(DequeueTaskMain, workerState, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default)
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                _tokenSource.Cancel();
            }
        }
    }
}
