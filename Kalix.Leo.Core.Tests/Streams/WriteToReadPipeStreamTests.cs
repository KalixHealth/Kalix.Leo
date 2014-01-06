using Kalix.Leo.Streams;
using NUnit.Framework;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Linq;

namespace Kalix.Leo.Core.Tests.Streams
{
    [TestFixture]
    public class WriteToReadPipeStreamTests
    {
        private const long KB = 1024;
        private const long MB = 1024 * KB;
        private static Random _random = new Random();

        [Test]
        public void CanFullyTransferData()
        {
            var lotsOfData = RandomData(MB);
            var result = new MemoryStream();

            using (var ms = new MemoryStream(lotsOfData))
            using (var writeReadStream = new WriteToReadPipeStream(4 * (int)KB))
            {
                var task1 = ms.CopyToAsync(writeReadStream).ContinueWith(t => writeReadStream.FinishWriting());
                var task2 = writeReadStream.CopyToAsync(result);

                task1.Wait();
                task2.Wait();
            }

            Assert.IsTrue(result.ToArray().SequenceEqual(lotsOfData));
        }

        [Test]
        public void WhenWritingAChunkOfDataShouldOnlyReturnMaxBufferSizeAtATime()
        {
            var lotsOfData = RandomData(MB);

            using (var ms = new MemoryStream(lotsOfData))
            using (var writeReadStream = new WriteToReadPipeStream(4 * (int)KB))
            {
                var copyTask = ms.CopyToAsync(writeReadStream);

                var firstChunk = new byte[10 * KB];
                var readData = writeReadStream.ReadAsync(firstChunk, 0, firstChunk.Length).Result;

                Assert.IsFalse(copyTask.IsCompleted);
                Assert.AreEqual(4 * (int)KB, readData);
                for (int i = 0; i < readData; i++)
                {
                    Assert.AreEqual(lotsOfData[i], firstChunk[i]);
                }
            }
        }

        private static byte[] RandomData(long noOfBytes)
        {
            var data = new byte[noOfBytes];
            _random.NextBytes(data);
            return data;
        }
    }
}
