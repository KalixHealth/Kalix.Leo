using Kalix.Leo.Queue;
using Kalix.Leo.Storage;
using Newtonsoft.Json;
using System;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public class BackupListener : IBackupListener
    {
        private readonly IQueue _backupQueue;
        private readonly IStore _backupStore;
        private readonly IStore _originalStore;
        
        public BackupListener(IQueue backupQueue, IStore originalStore, IStore backupStore)
        {
            _backupQueue = backupQueue;
            _backupStore = backupStore;
            _originalStore = originalStore;
        }

        public IDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null)
        {
            return _backupQueue.ListenForMessages(uncaughtException, messagesToProcessInParallel)
                .Select(m => Observable
                    .FromAsync(() => MessageRecieved(m))
                    .Catch((Func<Exception, IObservable<Unit>>)(e =>
                    {
                        if (uncaughtException != null) { uncaughtException(e); }
                        return Observable.Empty<Unit>();
                    }))) // Make sure this listener doesnt stop due to errors!
                .Merge()
                .Subscribe(); // Start listening
        }

        private async Task MessageRecieved(IQueueMessage message)
        {
            using (message)
            {
                var details = JsonConvert.DeserializeObject<StoreDataDetails>(message.Message);
                var location = details.GetLocation();

                using (var data = await _originalStore.LoadData(location))
                {
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

                await message.Complete();
            }
        }
    }
}
