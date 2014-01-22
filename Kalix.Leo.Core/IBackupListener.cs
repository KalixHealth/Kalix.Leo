using System;

namespace Kalix.Leo
{
    public interface IBackupListener
    {
        IDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null);
    }
}
