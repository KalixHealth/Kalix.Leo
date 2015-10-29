using Kalix.Leo.Queue;
using Kalix.Leo.Storage;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Listeners
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
            var maxMessages = messagesToProcessInParallel ?? Environment.ProcessorCount;
            var token = new CancellationTokenSource();
            var ct = token.Token;

            Task.Run(async () =>
            {
                while(!ct.IsCancellationRequested)
                {
                    try
                    {
                        var messages = await _backupQueue.ListenForNextMessage(maxMessages, ct).ConfigureAwait(false);
                        if (messages.Any())
                        {
                            await Task.WhenAll(messages.Select(MessageRecieved)).ConfigureAwait(false);
                        }
                        else
                        {
                            await Task.Delay(2000, ct).ConfigureAwait(false);
                        }
                    }
                    catch(Exception e)
                    {
                        if(uncaughtException != null)
                        {
                            uncaughtException(e);
                        }
                    }
                }
            }, ct);

            return token;
        }

        private async Task MessageRecieved(IQueueMessage message)
        {
            using(message)
            {
                var details = JsonConvert.DeserializeObject<StoreDataDetails>(message.Message);
                var location = details.GetLocation();
                var data = await _originalStore.LoadData(location).ConfigureAwait(false);

                if (data == null)
                {
                    // Need to make sure to soft delete in our backup...
                    await _backupStore.SoftDelete(location).ConfigureAwait(false);
                }
                else
                {
                    // Just save it right back into the backup!
                    await _backupStore.SaveData(location, data.Metadata, async (s, ct) => 
                    {
                        await data.Stream.CopyToAsync(s, ct).ConfigureAwait(false);
                        return data.Metadata.ContentLength;
                    }, CancellationToken.None).ConfigureAwait(false);
                    data.Stream.Dispose();
                }

                await message.Complete().ConfigureAwait(false);
            }
        }
    }
}
