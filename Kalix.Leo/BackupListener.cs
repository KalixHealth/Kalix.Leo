using Kalix.Leo.Queue;
using Kalix.Leo.Storage;
using Kalix.Leo.Streams;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
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
            _listener = backupQueue.SetupMessageListener(MessageRecieved, uncaughtException, messagesToProcessInParallel);
        }

        private async Task MessageRecieved(string message)
        {
            var data = JsonConvert.DeserializeObject<StoreDataDetails>(message);
            var location = data.GetLocation();

            var streamTasks = new List<Task>();

            using (var readWrite = new WriteToReadPipeStream())
            {
                Task saveTask = null;

                var hasFile = await _originalStore.LoadData(location, m => 
                {
                    // Make sure to save with the latest metadata
                    saveTask = _backupStore.SaveData(readWrite, location, m);
                    return readWrite;
                });

                if(hasFile)
                {
                    // If we have a file the save task will exist
                    readWrite.FinishWriting();
                    await saveTask;
                }
                else
                {
                    // Need to make sure to soft delete in our backup...
                    await _backupStore.SoftDelete(location);
                }
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
