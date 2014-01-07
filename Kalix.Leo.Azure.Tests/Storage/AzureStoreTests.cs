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
            _store = new AzureStore(CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient(), true);

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
        public class GetMetadataMethod : AzureStoreTests
        {
            [Test]
            public void NoFileReturnsNull()
            {
                var result = _store.GetMetadata(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void FindsMetadataIncludingSizeAndLength()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string>() { { "metadata1", "somemetadata" } }).Wait();

                var result = _store.GetMetadata(_location).Result;

                Assert.AreEqual("1048576", result[MetadataConstants.SizeMetadataKey]);
                Assert.IsTrue(result.ContainsKey(MetadataConstants.ModifiedMetadataKey));
                Assert.AreEqual("somemetadata", result["metadata1"]);
            }
        }

        [TestFixture]
        public class LoadDataMethod : AzureStoreTests
        {
            [Test]
            public void NullStreamCancelsTheDownload()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();

                var hasFile = _store.LoadData(_location, m => null).Result;
                Assert.IsFalse(hasFile);
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

                var snapshots = _store.FindSnapshots(_location).Result;

                Assert.AreEqual(1, snapshots.Count());
            }

            [Test]
            public void SubItemBlobSnapshotsAreNotIncluded()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();

                var blob2 = AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data", true);
                var location2 = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data");
                data.Position = 0;

                _store.SaveData(data, location2).Wait();

                var snapshots = _store.FindSnapshots(_location).Result;

                Assert.AreEqual(1, snapshots.Count());
            }

            [Test]
            public void SnapshotsAreFromNewestToOldest()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                _store.SaveData(data, _location).Wait();

                var snapshots = _store.FindSnapshots(_location).Result;

                var snapshot = snapshots.First().Modified;
                var snapshot2 = snapshots.Last().Modified;
                Assert.Less(snapshot2, snapshot);
            }
        }

        [TestFixture]
        public class LoadDataMethodWithSnapshot : AzureStoreTests
        {
            [Test]
            public void NullStreamCancelsTheDownload()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                var shapshot = _store.FindSnapshots(_location).Result.Single().Id;

                var hasFile = _store.LoadData(_location, m => null, shapshot).Result;
                Assert.IsFalse(hasFile);
            }

            [Test]
            public void MetadataIsTransferedWhenSelectingAStream()
            {
                var data = new MemoryStream(AzureTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string> { { "metadata1", "metadata" } }).Wait();
                var shapshot = _store.FindSnapshots(_location).Result.Single().Id;

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
                var result = _store.LoadData(_location, m => null, DateTime.UtcNow.Ticks.ToString()).Result;
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
                var shapshot = _store.FindSnapshots(_location).Result.Single().Id;

                _store.SoftDelete(_location).Wait();

                var result = _store.LoadData(_location, m => new MemoryStream(), shapshot).Result;
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
                var shapshot = _store.FindSnapshots(_location).Result.Single().Id;

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location, m => new MemoryStream(), shapshot).Result;
                Assert.IsFalse(result);
            }
        }
    }
}
