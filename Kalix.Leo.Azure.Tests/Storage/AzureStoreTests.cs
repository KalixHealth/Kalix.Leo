//using Kalix.Leo.Azure.Storage;
//using Kalix.Leo.Storage;
//using Microsoft.WindowsAzure.Storage;
//using Microsoft.WindowsAzure.Storage.Blob;
//using NUnit.Framework;
//using System;
//using System.Linq;
//using System.Reactive.Linq;

//namespace Kalix.Leo.Azure.Tests.Storage
//{
//    [TestFixture]
//    public class AzureStoreTests
//    {
//        protected AzureStore _store;
//        protected CloudBlockBlob _blob;
//        protected StoreLocation _location;

//        [SetUp]
//        public virtual void Init()
//        {
//            _store = new AzureStore(CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient(), true);

//            _blob = AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata", true);
//            _location = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata");
//        }

//        [TestFixture]
//        public class SaveDataMethod : AzureStoreTests
//        {
//            [Test]
//            public void HasMetadataCorrectlySavesIt()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                var m = new Metadata();
//                m["metadata1"] = "somemetadata";
//                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

//                _blob.FetchAttributes();
//                Assert.AreEqual("somemetadata", _blob.Metadata["metadata1"]);
//            }

//            [Test]
//            public void AlwaysOverridesMetadata()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                var m = new Metadata();
//                m["metadata1"] = "somemetadata";
//                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

//                var m2 = new Metadata();
//                m2["metadata2"] = "othermetadata";
//                _store.SaveData(_location, new DataWithMetadata(data, m2)).Wait();

//                _blob.FetchAttributes();
//                Assert.IsFalse(_blob.Metadata.ContainsKey("metadata1"));
//                Assert.AreEqual("othermetadata", _blob.Metadata["metadata2"]);
//            }
//        }

//        [TestFixture]
//        public class GetMetadataMethod : AzureStoreTests
//        {
//            [Test]
//            public void NoFileReturnsNull()
//            {
//                var result = _store.GetMetadata(_location).Result;
//                Assert.IsNull(result);
//            }

//            [Test]
//            public void FindsMetadataIncludingSizeAndLength()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                var m = new Metadata();
//                m["metadata1"] = "somemetadata";
//                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

//                var result = _store.GetMetadata(_location).Result;

//                Assert.AreEqual("1048576", result[MetadataConstants.SizeMetadataKey]);
//                Assert.IsTrue(result.ContainsKey(MetadataConstants.ModifiedMetadataKey));
//                Assert.AreEqual("somemetadata", result["metadata1"]);
//            }
//        }

//        [TestFixture]
//        public class LoadDataMethod : AzureStoreTests
//        {
//            [Test]
//            public void MetadataIsTransferedWhenSelectingAStream()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                var m = new Metadata();
//                m["metadata1"] = "metadata";
//                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

//                var result = _store.LoadData(_location).Result;

//                Assert.AreEqual("metadata", result.Metadata["metadata1"]);
//            }

//            [Test]
//            public void NoFileReturnsFalse()
//            {
//                var result = _store.LoadData(_location).Result;
//                Assert.IsNull(result);
//            }

//            [Test]
//            public void FileMarkedAsDeletedReturnsFalse()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                var m = new Metadata();
//                m["leo.azurestorage.deleted"] = DateTime.UtcNow.Ticks.ToString();
//                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

//                var result = _store.LoadData(_location).Result;
//                Assert.IsNull(result);
//            }
//        }

//        [TestFixture]
//        public class FindSnapshotsMethod : AzureStoreTests
//        {
//            [Test]
//            public void NoSnapshotsReturnsEmpty()
//            {
//                var snapshots = _store.FindSnapshots(_location).ToEnumerable();

//                Assert.AreEqual(0, snapshots.Count());
//            }

//            [Test]
//            public void SingleSnapshotCanBeFound()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                var m = new Metadata();
//                m["metadata1"] = "metadata";
//                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

//                var snapshots = _store.FindSnapshots(_location).ToEnumerable();

//                Assert.AreEqual(1, snapshots.Count());
//            }

//            [Test]
//            public void SubItemBlobSnapshotsAreNotIncluded()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                _store.SaveData(_location, new DataWithMetadata(data)).Wait();

//                var blob2 = AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data", true);
//                var location2 = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data");

//                _store.SaveData(location2, new DataWithMetadata(data)).Wait();

//                var snapshots = _store.FindSnapshots(_location).ToEnumerable();

//                Assert.AreEqual(1, snapshots.Count());
//            }
//        }

//        [TestFixture]
//        public class LoadDataMethodWithSnapshot : AzureStoreTests
//        {
//            [Test]
//            public void MetadataIsTransferedWhenSelectingAStream()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                var m = new Metadata();
//                m["metadata1"] = "metadata";
//                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();
//                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

//                var res = _store.LoadData(_location, shapshot).Result;

//                Assert.AreEqual("metadata", res.Metadata["metadata1"]);
//            }

//            [Test]
//            public void NoFileReturnsFalse()
//            {
//                var result = _store.LoadData(_location, DateTime.UtcNow.Ticks.ToString()).Result;
//                Assert.IsNull(result);
//            }
//        }

//        [TestFixture]
//        public class SoftDeleteMethod : AzureStoreTests
//        {
//            [Test]
//            public void BlobThatDoesNotExistShouldNotThrowError()
//            {
//                _store.SoftDelete(_location).Wait();
//            }

//            [Test]
//            public void BlobThatIsSoftDeletedShouldNotBeLoadable()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                _store.SaveData(_location, new DataWithMetadata(data)).Wait();

//                _store.SoftDelete(_location).Wait();

//                var result = _store.LoadData(_location).Result;
//                Assert.IsNull(result);
//            }

//            [Test]
//            public void ShouldNotDeleteSnapshots()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                _store.SaveData(_location, new DataWithMetadata(data)).Wait();
//                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

//                _store.SoftDelete(_location).Wait();

//                var result = _store.LoadData(_location, shapshot).Result;
//                Assert.IsNotNull(result);
//            }
//        }

//        [TestFixture]
//        public class PermanentDeleteMethod : AzureStoreTests
//        {
//            [Test]
//            public void BlobThatDoesNotExistShouldNotThrowError()
//            {
//                _store.PermanentDelete(_location).Wait();
//            }

//            [Test]
//            public void BlobThatIsSoftDeletedShouldNotBeLoadable()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                _store.SaveData(_location, new DataWithMetadata(data)).Wait();

//                _store.PermanentDelete(_location).Wait();

//                var result = _store.LoadData(_location).Result;
//                Assert.IsNull(result);
//            }

//            [Test]
//            public void ShouldDeleteAllSnapshots()
//            {
//                var data = Observable.Return(AzureTestsHelper.RandomData(1));
//                _store.SaveData(_location, new DataWithMetadata(data)).Wait();
//                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

//                _store.PermanentDelete(_location).Wait();

//                var result = _store.LoadData(_location, shapshot).Result;
//                Assert.IsNotNull(result);
//            }
//        }
//    }
//}
