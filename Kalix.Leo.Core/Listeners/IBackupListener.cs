using System;

namespace Kalix.Leo.Listeners
{
    public interface IBackupListener
    {
        IDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null);
    }
}
