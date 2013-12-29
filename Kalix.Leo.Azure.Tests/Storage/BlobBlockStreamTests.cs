using Kalix.Leo.Azure.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;

namespace Kalix.Leo.Azure.Tests.Storage
{
    [TestFixture]
    public class BlobBlockStreamTests
    {
        private CloudBlockBlob _blob;
        private OperationContext _context;
        private BlobBlockStream _stream;

        [SetUp]
        public void Init()
        {
            _blob = AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "BlobBlockStreamTests.testdata", true);

            _context = new OperationContext();
            _stream = new BlobBlockStream(_blob, _context);
        }

        [Test]
        public void WriteLessThan4MbNoCalls()
        {
            int reqCount = 0;
            _context.SendingRequest += (e, a) => reqCount++;

            var rand = AzureTestsHelper.RandomData(3);
            _stream.Write(rand, 0, rand.Length);

            Assert.AreEqual(0, reqCount);
        }

        [Test]
        public void WriteLessThan4mbAndClose2Calls()
        {
            int reqCount = 0;
            _context.SendingRequest += (e, a) => reqCount++;

            var rand = AzureTestsHelper.RandomData(3);
            _stream.Write(rand, 0, rand.Length);
            _stream.Dispose();

            Assert.AreEqual(2, reqCount);
        }

        [Test]
        public void Write9mbAndClose4Calls()
        {
            int reqCount = 0;
            _context.SendingRequest += (e, a) => reqCount++;

            var rand = AzureTestsHelper.RandomData(9);
            _stream.Write(rand, 0, rand.Length);
            _stream.Dispose();

            Assert.AreEqual(4, reqCount);
        }

        [Test]
        public void Write1mbAndCanReadData()
        {
            var rand = AzureTestsHelper.RandomData(1);
            _stream.Write(rand, 0, rand.Length);
            _stream.Dispose();

            var newData = new byte[rand.Length];
            _blob.DownloadToByteArray(newData, 0);

            for(var i = 0; i < rand.Length; i++)
            {
                Assert.AreEqual(rand[i], newData[i]);
            }
        }
    }
}
