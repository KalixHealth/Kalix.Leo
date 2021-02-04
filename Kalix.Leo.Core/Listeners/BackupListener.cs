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
        private static readonly TimeSpan VisibilityTimeout = TimeSpan.FromHours(1);
        private static readonly TimeSpan DelayTime = TimeSpan.FromSeconds(2);

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

            Task.Run(() => _backupQueue.ListenForMessages(maxMessages, VisibilityTimeout, DelayTime, uncaughtException, ct)
                .ForEachAwaitAsync(m => MessageRecieved(m, uncaughtException), ct)
            );

            return token;
        }

        private async Task MessageRecieved(IQueueMessage message, Action<Exception> uncaughtException)
        {
            try
            {
                var details = JsonConvert.DeserializeObject<StoreDataDetails>(message.Message);
                var location = details.GetLocation();
                var data = await _originalStore.LoadData(location);

                if (data == null)
                {
                    // Need to make sure to soft delete in our backup...
                    // We don't have metadata about who did it though...
                    await _backupStore.SoftDelete(location, new UpdateAuditInfo());
                }
                else
                {
                    // Just save it right back into the backup!
                    var ct = CancellationToken.None;
                    await _backupStore.SaveData(location, data.Metadata, data.Metadata.Audit.ToUpdateAuditInfo(), async (s) =>
                    {
                        await data.Reader.CopyToAsync(s, ct);
                        return data.Metadata.ContentLength;
                    }, ct);
                }

                await message.Complete();
            }
            catch (Exception e)
            {
                if (uncaughtException != null) { uncaughtException(e); }
            }
            finally
            {
                await message.DisposeAsync();
            }
        }
    }
}
