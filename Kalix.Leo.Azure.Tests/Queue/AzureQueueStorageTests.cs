using Kalix.Leo.Azure.Queue;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using NUnit.Framework;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace Kalix.Leo.Azure.Tests.Queue
{
    [TestFixture]
    public class AzureQueueStorageTests
    {
        protected CloudQueue _queue;
        protected AzureQueueStorage _azureQueue;

        [SetUp]
        public virtual async Task Init()
        {
            var client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudQueueClient();
            _queue = client.GetQueueReference("kalixleotestqueue");
            await _queue.CreateIfNotExistsAsync();

            _azureQueue = new AzureQueueStorage(client, "kalixleotestqueue");
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

                var messages = (await _azureQueue.ListenForNextMessage(30, TimeSpan.FromMinutes(1), CancellationToken.None)).ToList();

                Assert.AreEqual(1, messages.Count);
                Assert.AreEqual("test", messages[0].Message);
            }

            [Test]
            public async Task UseVisibilityIfPassed()
            {
                await _azureQueue.SendMessage("test", TimeSpan.FromSeconds(2));

                var messages = (await _azureQueue.ListenForNextMessage(30, TimeSpan.FromMinutes(1), CancellationToken.None)).ToList();

                Assert.AreEqual(0, messages.Count);

                await Task.Delay(3000);

                messages = (await _azureQueue.ListenForNextMessage(30, TimeSpan.FromMinutes(1), CancellationToken.None)).ToList();
                Assert.AreEqual(1, messages.Count);
                Assert.AreEqual("test", messages[0].Message);
            }
        }
    }
}
