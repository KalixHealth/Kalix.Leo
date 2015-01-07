using Kalix.Leo.Azure.Storage;
using Kalix.Leo.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;
using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;

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
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                _store.SaveData(_location, m, s => s.WriteAsync(data, 0, data.Length)).Wait();

                _blob.FetchAttributes();
                Assert.AreEqual("somemetadata", _blob.Metadata["metadata1"]);
            }

            [Test]
            public void AlwaysOverridesMetadata()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                _store.SaveData(_location, m, s => s.WriteAsync(data, 0, data.Length)).Wait();

                var m2 = new Metadata();
                m2["metadata2"] = "othermetadata";
                _store.SaveData(_location, m2, s => s.WriteAsync(data, 0, data.Length)).Wait();

                _blob.FetchAttributes();
                Assert.IsFalse(_blob.Metadata.ContainsKey("metadata1"));
                Assert.AreEqual("othermetadata", _blob.Metadata["metadata2"]);
            }

            [Test]
            public void MultiUploadLargeFileIsSuccessful()
            {
                var data = AzureTestsHelper.RandomData(7);
                _store.SaveData(_location, null, s => s.WriteAsync(data, 0, data.Length)).Wait();

                Assert.IsTrue(_blob.Exists());
            }
        }

        [TestFixture]
        public class TryOptimisticWriteMethod : AzureStoreTests
        {
            [Test]
            public void HasMetadataCorrectlySavesIt()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                var success = _store.TryOptimisticWrite(_location, m, s => s.WriteAsync(data, 0, data.Length)).Result;

                _blob.FetchAttributes();
                Assert.IsTrue(success);
                Assert.AreEqual("somemetadata", _blob.Metadata["metadata1"]);
            }

            [Test]
            public void AlwaysOverridesMetadata()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                var success1 = _store.TryOptimisticWrite(_location, m, s => s.WriteAsync(data, 0, data.Length)).Result;
                var oldMetadata = _store.GetMetadata(_location).Result;

                var m2 = new Metadata();
                m2.ETag = oldMetadata.ETag;
                m2["metadata2"] = "othermetadata";
                var success2 = _store.TryOptimisticWrite(_location, m2, s => s.WriteAsync(data, 0, data.Length)).Result;

                _blob.FetchAttributes();
                Assert.IsTrue(success1, "first write failed");
                Assert.IsTrue(success2, "second write failed");
                Assert.IsFalse(_blob.Metadata.ContainsKey("metadata1"));
                Assert.AreEqual("othermetadata", _blob.Metadata["metadata2"]);
            }

            [Test]
            public void NoETagMustBeNewSave()
            {
                var data = AzureTestsHelper.RandomData(1);
                var success1 = _store.TryOptimisticWrite(_location, null, s => s.WriteAsync(data, 0, data.Length)).Result;
                var success2 = _store.TryOptimisticWrite(_location, null, s => s.WriteAsync(data, 0, data.Length)).Result;

                Assert.IsTrue(success1, "first write failed");
                Assert.IsFalse(success2, "second write succeeded");
            }

            [Test]
            public void ETagDoesNotMatchFails()
            {
                var data = AzureTestsHelper.RandomData(1);
                var metadata = new Metadata { ETag = "notreal" };
                var success = _store.TryOptimisticWrite(_location, metadata, s => s.WriteAsync(data, 0, data.Length)).Result;

                Assert.IsFalse(success, "write should not have succeeded with fake eTag");
            }

            [Test]
            public void MultiUploadLargeFileIsSuccessful()
            {
                var data = AzureTestsHelper.RandomData(7);
                var success = _store.TryOptimisticWrite(_location, null, s => s.WriteAsync(data, 0, data.Length)).Result;

                Assert.IsTrue(success);
                Assert.IsTrue(_blob.Exists());
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
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                _store.SaveData(_location, m, s => s.WriteAsync(data, 0, data.Length)).Wait();

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
            public void MetadataIsTransferedWhenSelectingAStream()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "metadata";
                _store.SaveData(_location, m, s => s.WriteAsync(data, 0, data.Length)).Wait();

                var result = _store.LoadData(_location).Result;
                Assert.AreEqual("metadata", result.Metadata["metadata1"]);
            }

            [Test]
            public void NoFileReturnsFalse()
            {
                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void NoContainerReturnsFalse()
            {
                var result = _store.LoadData(new StoreLocation("blahblahblah", "blah")).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void FileMarkedAsDeletedReturnsNull()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["leodeleted"] = DateTime.UtcNow.Ticks.ToString();
                _store.SaveData(_location, m, s => s.WriteAsync(data, 0, data.Length)).Wait();

                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void AllDataLoadsCorrectly()
            {
                var data = AzureTestsHelper.RandomData(1);
                _store.SaveData(_location, null, s => s.WriteAsync(data, 0, data.Length)).Wait();

                var result = _store.LoadData(_location).Result;
                byte[] resData;
                using(var ms = new MemoryStream())
                {
                    result.Stream.CopyTo(ms);
                    resData = ms.ToArray();
                }
                Assert.IsTrue(data.SequenceEqual(resData));
            }
        }

        [TestFixture]
        public class FindSnapshotsMethod : AzureStoreTests
        {
            [Test]
            public void NoSnapshotsReturnsEmpty()
            {
                var snapshots = _store.FindSnapshots(_location).ToEnumerable();

                Assert.AreEqual(0, snapshots.Count());
            }

            [Test]
            public void SingleSnapshotCanBeFound()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "metadata";
                _store.SaveData(_location, m, s => s.WriteAsync(data, 0, data.Length)).Wait();

                var snapshots = _store.FindSnapshots(_location).ToEnumerable();

                Assert.AreEqual(1, snapshots.Count());
            }

            [Test]
            public void SubItemBlobSnapshotsAreNotIncluded()
            {
                var data = AzureTestsHelper.RandomData(1);
                _store.SaveData(_location, null, s => s.WriteAsync(data, 0, data.Length)).Wait();

                var blob2 = AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data", true);
                var location2 = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data");

                _store.SaveData(location2, null, s => s.WriteAsync(data, 0, data.Length)).Wait();

                var snapshots = _store.FindSnapshots(_location).ToEnumerable();

                Assert.AreEqual(1, snapshots.Count());
            }
        }

        [TestFixture]
        public class LoadDataMethodWithSnapshot : AzureStoreTests
        {
            [Test]
            public void MetadataIsTransferedWhenSelectingAStream()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "metadata";
                _store.SaveData(_location, m, s => s.WriteAsync(data, 0, data.Length)).Wait();
                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

                var res = _store.LoadData(_location, shapshot).Result;
                Assert.AreEqual("metadata", res.Metadata["metadata1"]);
            }

            [Test]
            public void NoFileReturnsFalse()
            {
                var result = _store.LoadData(_location, DateTime.UtcNow.Ticks.ToString()).Result;
                Assert.IsNull(result);
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
                var data = AzureTestsHelper.RandomData(1);
                _store.SaveData(_location, null, s => s.WriteAsync(data, 0, data.Length)).Wait();

                _store.SoftDelete(_location).Wait();

                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void ShouldNotDeleteSnapshots()
            {
                var data = AzureTestsHelper.RandomData(1);
                _store.SaveData(_location, null, s => s.WriteAsync(data, 0, data.Length)).Wait();
                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

                _store.SoftDelete(_location).Wait();

                var result = _store.LoadData(_location, shapshot).Result;
                Assert.IsNotNull(result);
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
                var data = AzureTestsHelper.RandomData(1);
                _store.SaveData(_location, null, s => s.WriteAsync(data, 0, data.Length)).Wait();

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void ShouldDeleteAllSnapshots()
            {
                var data = AzureTestsHelper.RandomData(1);
                _store.SaveData(_location, null, s => s.WriteAsync(data, 0, data.Length)).Wait();
                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location, shapshot).Result;
                Assert.IsNull(result);
            }
        }

        [TestFixture]
        public class LockMethod : AzureStoreTests
        {
            [Test]
            public void LockSuceedsEvenIfNoFile()
            {
                using(var l = _store.Lock(_location).Result)
                {
                    Assert.IsNotNull(l);
                }
            }

            [Test]
            public void IfAlreadyLockedOtherLocksFail()
            {
                using(var l = _store.Lock(_location).Result)
                using(var l2 = _store.Lock(_location).Result)
                {
                    Assert.IsNotNull(l);
                    Assert.IsNull(l2);
                }
            }

            [Test]
            [ExpectedException(typeof(LockException))]
            public void IfFileLockedReturnsFalse()
            {
                using (var l = _store.Lock(_location).Result)
                {
                    var data = AzureTestsHelper.RandomData(1);
                    try
                    {
                        _store.SaveData(_location, null, s => s.WriteAsync(data, 0, data.Length)).Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                }
            }
        }
    }
}
