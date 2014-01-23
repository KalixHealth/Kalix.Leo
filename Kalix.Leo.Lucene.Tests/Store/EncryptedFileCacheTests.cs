using Kalix.Leo.Lucene.Store;
using NUnit.Framework;
using System;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene.Tests.Store
{
    [TestFixture]
    public class EncryptedFileCacheTests
    {
        private EncryptedFileCache _cache;

        [SetUp]
        public void Init()
        {
            _cache = new EncryptedFileCache();
        }

        [TearDown]
        public void Destroy()
        {
            _cache.Dispose();
        }

        [Test]
        public void ReadWriteAllGood()
        {
            var data = RandomData(2);
            _cache.UpdateIfModified("test", Task.FromResult(new DataWithMetadata(Observable.Return(data)))).Wait();

            using (var result = _cache.LoadAllData("test").Result)
            {
                Assert.AreEqual(2048, result.Metadata.Size);
            }
        }

        private static Random _random = new Random();
        public static byte[] RandomData(long noOfKb)
        {
            var data = new byte[noOfKb * 1024];
            _random.NextBytes(data);
            return data;
        }
    }
}
