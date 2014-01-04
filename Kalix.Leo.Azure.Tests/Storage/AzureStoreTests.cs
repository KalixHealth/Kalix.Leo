using Kalix.Leo.Azure.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests.Storage
{
    [TestFixture]
    public class AzureStoreTests
    {
        protected AzureStore _store;
        protected CloudBlockBlob _blob;
        protected StoreLocation _location;

        [SetUp]
        public virtual void Init()
        {
            _store = new AzureStore(CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient());

            _blob = AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata", true);
            _location = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata");
        }
        
        [TestFixture]
        public class SaveDataMethod : AzureStoreTests
        {
            [Test]
            public void HasMetadataCorrectlySavesIt()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string>() { { "metadata1", "somemetadata" } }).Wait();

                _blob.FetchAttributes();
                Assert.AreEqual("somemetadata", _blob.Metadata["metadata1"]);
            }

            [Test]
            public void AlwaysOverridesMetadata()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string>() { { "metadata1", "somemetadata" } }).Wait();
                
                data.Position = 0;
                _store.SaveData(data, _location, new Dictionary<string, string>() { { "metadata2", "othermetadata" } }).Wait();

                _blob.FetchAttributes();
                Assert.IsFalse(_blob.Metadata.ContainsKey("metadata1"));
                Assert.AreEqual("othermetadata", _blob.Metadata["metadata2"]);
            }
        }

        [TestFixture]
        public class LoadDataMethod : AzureStoreTests
        {
            [Test]
            [ExpectedException(typeof(TaskCanceledException))]
            public void NullStreamCancelsTheDownload()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();

                try
                {
                    _store.LoadData(_location, m => null).Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException;
                }
            }

            [Test]
            public void MetadataIsTransferedWhenSelectingAStream()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string> { { "metadata1", "metadata" } }).Wait();

                string metadata = null;
                var result = _store.LoadData(_location, m =>
                {
                    metadata = m["metadata1"];
                    return new MemoryStream();
                }).Result;

                Assert.IsTrue(result);
                Assert.AreEqual("metadata", metadata);
            }

            [Test]
            public void NoFileReturnsFalse()
            {
                var result = _store.LoadData(_location, m => null).Result;
                Assert.IsFalse(result);
            }

            [Test]
            public void FileMarkedAsDeletedReturnsFalse()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string> { { "azurestorage_deleted", DateTime.UtcNow.Ticks.ToString() } }).Wait();

                var result = _store.LoadData(_location, m => null).Result;
                Assert.IsFalse(result);
            }
        }

        [TestFixture]
        public class TakeSnapshotMethod : AzureStoreTests
        {
            [Test]
            public void NoBlobReturnsNull()
            {
                var result = _store.TakeSnapshot(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void CreateSnapshopSavesMetadata()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string> { { "metadata1", "metadata" } }).Wait();

                var snapshot = _store.TakeSnapshot(_location).Result.Value;

                _store.SaveData(data, _location).Wait();

                string metadata = null;
                _store.LoadData(_location, m =>
                {
                    metadata = m["metadata1"];
                    return new MemoryStream();
                }, snapshot).Wait();

                Assert.AreEqual("metadata", metadata);
            }
        }

        [TestFixture]
        public class FindSnapshotsMethod : AzureStoreTests
        {
            [Test]
            public void NoSnapshotsReturnsEmpty()
            {
                var snapshots = _store.FindSnapshots(_location).Result;

                Assert.AreEqual(0, snapshots.Count());
            }

            [Test]
            public void SingleSnapshotCanBeFound()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string> { { "metadata1", "metadata" } }).Wait();
                var snapshot = _store.TakeSnapshot(_location).Result;

                var snapshots = _store.FindSnapshots(_location).Result;

                Assert.AreEqual(snapshot, snapshots.First());
            }

            [Test]
            public void SubItemBlobSnapshotsAreNotIncluded()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                var snapshot = _store.TakeSnapshot(_location).Result;

                var blob2 = AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data", true);
                var location2 = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data");
                data.Position = 0;

                _store.SaveData(data, location2).Wait();
                var snapshot2 = _store.TakeSnapshot(location2).Result;

                var snapshots = _store.FindSnapshots(_location).Result;

                Assert.AreEqual(1, snapshots.Count());
            }

            [Test]
            public void SnapshotsAreFromNewestToOldest()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                var snapshot = _store.TakeSnapshot(_location).Result;
                var snapshot2 = _store.TakeSnapshot(_location).Result;

                var snapshots = _store.FindSnapshots(_location).Result;

                Assert.AreEqual(snapshot, snapshots.Last());
                Assert.AreEqual(snapshot2, snapshots.First());
            }
        }

        [TestFixture]
        public class LoadDataMethodWithSnapshot : AzureStoreTests
        {
            [Test]
            [ExpectedException(typeof(TaskCanceledException))]
            public void NullStreamCancelsTheDownload()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                var shapshot = _store.TakeSnapshot(_location).Result.Value;

                try
                {
                    _store.LoadData(_location, m => null, shapshot).Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException;
                }
            }

            [Test]
            public void MetadataIsTransferedWhenSelectingAStream()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string> { { "metadata1", "metadata" } }).Wait();
                var shapshot = _store.TakeSnapshot(_location).Result.Value;

                string metadata = null;
                _store.LoadData(_location, m =>
                {
                    metadata = m["metadata1"];
                    return new MemoryStream();
                }, shapshot).Wait();

                Assert.AreEqual("metadata", metadata);
            }

            [Test]
            public void NoFileReturnsFalse()
            {
                var result = _store.LoadData(_location, m => null, DateTime.UtcNow).Result;
                Assert.IsFalse(result);
            }
        }

        [TestFixture]
        public class SoftDeleteMethod : AzureStoreTests
        {
            [Test]
            public void BlobThatDoesNotExistShouldNotThrowError()
            {
                _store.SoftDelete(_location).Wait();
            }

            [Test]
            public void BlobThatIsSoftDeletedShouldNotBeLoadable()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();

                _store.SoftDelete(_location).Wait();

                var result = _store.LoadData(_location, m => null).Result;
                Assert.IsFalse(result);
            }

            [Test]
            public void ShouldNotDeleteSnapshots()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                var snapshot = _store.TakeSnapshot(_location).Result.Value;

                _store.SoftDelete(_location).Wait();

                var result = _store.LoadData(_location, m => new MemoryStream(), snapshot).Result;
                Assert.IsTrue(result);
            }
        }

        [TestFixture]
        public class PermanentDeleteMethod : AzureStoreTests
        {
            [Test]
            public void BlobThatDoesNotExistShouldNotThrowError()
            {
                _store.PermanentDelete(_location).Wait();
            }

            [Test]
            public void BlobThatIsSoftDeletedShouldNotBeLoadable()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location, m => null).Result;
                Assert.IsFalse(result);
            }

            [Test]
            public void ShouldDeleteAllSnapshots()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                var snapshot = _store.TakeSnapshot(_location).Result.Value;

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location, m => new MemoryStream(), snapshot).Result;
                Assert.IsFalse(result);
            }
        }
    }
}
