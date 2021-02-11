using System;

namespace Kalix.Leo.Listeners
{
    /// <summary>
    /// A listener used to listen to the backup queue
    /// </summary>
    public interface IBackupListener
    {
        /// <summary>
        /// Start a listener on a backup queue and handle the messages
        /// </summary>
        /// <param name="uncaughtException">Use this to listen to failed messages</param>
        /// <param name="messagesToProcessInParallel">Number of messages to handle concurrently</param>
        /// <returns>A disposable that will stop the listener if disposed</returns>
        IAsyncDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null);
    }
}
