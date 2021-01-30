using Azure.Storage.Queues;
using Kalix.Leo.Azure.Queue;
using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests.Queue
{
    [TestFixture]
    public class AzureQueueStorageTests
    {
        protected QueueClient _queue;
        protected AzureQueueStorage _azureQueue;

        [SetUp]
        public virtual async Task Init()
        {
            _queue = new QueueClient(AzureTestsHelper.DevelopmetStorage, "kalixleotestqueue");
            await _queue.CreateIfNotExistsAsync();

            _azureQueue = new AzureQueueStorage(_queue);
        }

        [TearDown]
        public virtual async Task TearDown()
        {
            await _queue.DeleteIfExistsAsync();
        }

        [TestFixture]
        public class SendMessageMethod : AzureQueueStorageTests
        {
            [Test]
            public async Task VisibleIfNoVisibility()
            {
                await _azureQueue.SendMessage("test");
                var messageTask = _azureQueue.ListenForMessages(30, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(1)).FirstOrDefaultAsync().AsTask();

                var t = await Task.WhenAny(Task.Delay(5000), messageTask);

                Assert.AreEqual(t, messageTask);
                Assert.AreEqual("test", messageTask.Result.Message);
            }

            [Test]
            public async Task UseVisibilityIfPassed()
            {
                await _azureQueue.SendMessage("test", TimeSpan.FromSeconds(5));

                var messageTask = _azureQueue.ListenForMessages(30, TimeSpan.FromMinutes(10), TimeSpan.FromSeconds(1)).FirstOrDefaultAsync().AsTask();
                var t = await Task.WhenAny(Task.Delay(3000), messageTask);

                Assert.AreNotEqual(t, messageTask);

                await Task.Delay(3000);
                
                t = await Task.WhenAny(Task.Delay(3000), messageTask);

                Assert.AreEqual(t, messageTask);
                Assert.AreEqual("test", messageTask.Result.Message);
            }
        }
    }
}
