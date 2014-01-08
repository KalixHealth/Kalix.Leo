using Kalix.Leo.Queue;
using Kalix.Leo.Storage;
using Newtonsoft.Json;
using System;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public sealed class BackupListener : IDisposable
    {
        private readonly IStore _backupStore;
        private readonly IStore _originalStore;
        private IDisposable _listener;

        public BackupListener(IQueue backupQueue, IStore originalStore, IStore backupStore, Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null)
        {
            _backupStore = backupStore;
            _originalStore = originalStore;

            // Setup the listener...
            _listener = backupQueue.ListenForMessages(uncaughtException, messagesToProcessInParallel)
                .Select(m => Observable
                    .FromAsync(() => MessageRecieved(m))
                    .Catch((Func<Exception,IObservable<Unit>>)(e => 
                    {
                        if(uncaughtException != null) { uncaughtException(e); }
                        return Observable.Empty<Unit>();
                    }))) // Make sure this listener doesnt stop due to errors!
                .Merge()
                .Subscribe(); // Start listening
        }

        private async Task MessageRecieved(string message)
        {
            var details = JsonConvert.DeserializeObject<StoreDataDetails>(message);
            var location = details.GetLocation();

            var data = await _originalStore.LoadData(location);
            if (data == null)
            {
                // Need to make sure to soft delete in our backup...
                await _backupStore.SoftDelete(location);
            }
            else
            {
                // Just save it right back into the backup!
                await _backupStore.SaveData(location, data);
            }
        }

        public void Dispose()
        {
            if(_listener != null)
            {
                _listener.Dispose();
                _listener = null;
            }
        }
    }
}
