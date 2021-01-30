using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Kalix.Leo.Queue;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public sealed class AzureQueueStorageMessage : IQueueMessage
    {
        private static readonly TimeSpan MaxVisibilityTimeout = TimeSpan.FromDays(7);

        private readonly CancellationTokenSource _tcs;
        private readonly Task _visiblityTask;
        private readonly QueueClient _queue;
        private readonly string _messageId;
        private string _popId;

        public AzureQueueStorageMessage(QueueClient queue, QueueMessage message, TimeSpan visiblityHoldTime)
        {
            if (visiblityHoldTime > MaxVisibilityTimeout) { visiblityHoldTime = MaxVisibilityTimeout; }

            _queue = queue;
            Message = message.Body.ToString();
            _messageId = message.MessageId;
            _popId = message.PopReceipt;
            IsComplete = false;

            _tcs = new CancellationTokenSource();
            _visiblityTask = Task.Run(() => HoldVisiblility(visiblityHoldTime, _tcs.Token));
        }

        public string Message { get; private set; }
        public bool IsComplete { get; private set; }

        public async Task<bool> Complete()
        {
            if (IsComplete) { return false; }

            IsComplete = true;
            _tcs.Cancel();
            try { await _visiblityTask; } catch { }

            try
            {
                await _queue.DeleteMessageAsync(_messageId, _popId);
                return true;
            }
            catch (RequestFailedException e) when (e.ErrorCode == QueueErrorCode.MessageNotFound)
            {
                return false;
            }
        }

        public async ValueTask DisposeAsync()
        {
            IsComplete = true;
            _tcs.Cancel();
            try { await _visiblityTask; } catch { }
            _tcs.Dispose();
        }

        private async Task HoldVisiblility(TimeSpan visibilityTimeout, CancellationToken token)
        {
            var refreshTime = TimeSpan.FromTicks(visibilityTimeout.Ticks / 2);

            try
            {
                while (!token.IsCancellationRequested && !IsComplete)
                {
                    await Task.Delay(refreshTime, token);

                    // Don't use cancellation token in next call on purpose, so always finishes if started
                    var update = await _queue.UpdateMessageAsync(_messageId, _popId, visibilityTimeout: visibilityTimeout);
                    _popId = update.Value.PopReceipt;
                }
            }
            catch (Exception)
            {
                IsComplete = true;
            }
        }
    }
}
