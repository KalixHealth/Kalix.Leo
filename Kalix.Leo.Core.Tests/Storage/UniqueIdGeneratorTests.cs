using Kalix.Leo.Storage;
using NSubstitute;
using NUnit.Framework;
using System.Threading.Tasks;

namespace Kalix.Leo.Core.Tests.Storage
{
    [TestFixture]
    public class UniqueIdGeneratorTests
    {
        protected IOptimisticStore _store;
        protected StoreLocation _loc;

        [SetUp]
        public void Init()
        {
            _loc = new StoreLocation();
            _store = Substitute.For<IOptimisticStore>();
        }

        [Test]
        public void FirstCallFetchesNewItems()
        {
            _store.LoadData(_loc).Returns(Task.FromResult<DataWithMetadata>(null));
            _store.TryOptimisticWrite(_loc, null, null).ReturnsForAnyArgs(Task.FromResult(true));

            var generator = GetGenerator(10);
            var id = generator.NextId().Result;

            _store.Received(1).LoadData(_loc);
            Assert.AreEqual(1, id);
        }

        [Test]
        public void GrabsTenItemsWithCallingOutMoreThanOnce()
        {
            _store.LoadData(_loc).Returns(Task.FromResult<DataWithMetadata>(null));
            _store.TryOptimisticWrite(_loc, null, null).ReturnsForAnyArgs(Task.FromResult(true));

            var generator = GetGenerator(10);
            long id = 0;
            for (int i = 0; i < 10; i++)
            {
                id = generator.NextId().Result;
            }

            _store.Received(1).LoadData(_loc);
            Assert.AreEqual(10, id);
        }

        private UniqueIdGenerator GetGenerator(int range)
        {
            return new UniqueIdGenerator(_store, _loc, range);
        }
    }
}
