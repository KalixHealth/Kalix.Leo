using Amazon.S3;
using Amazon.S3.Model;
using Kalix.Leo.Amazon.Storage;
using Kalix.Leo.Storage;
using NUnit.Framework;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reactive.Linq;

namespace Kalix.Leo.Amazon.Tests.Storage
{
    [TestFixture]
    public class AmazonStoreTests
    {
        protected string _bucket;
        protected AmazonStore _store;
        protected AmazonS3Client _client;
        protected StoreLocation _location;

        [SetUp]
        public virtual void Init()
        {
            _bucket = ConfigurationManager.AppSettings["TestBucket"];
            _client = AmazonTestsHelper.SetupBlob(_bucket, "kalix-leo-tests\\AmazonStoreTests.testdata");
            _location = new StoreLocation("kalix-leo-tests", "AmazonStoreTests.testdata");
            _store = new AmazonStore(_client, _bucket);
        }

        [TestFixture]
        public class SaveDataMethod : AmazonStoreTests
        {
            [Test]
            public void HasMetadataCorrectlySavesIt()
            {
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

                var metadata = GetMetadata(_location);
                Assert.AreEqual("somemetadata", metadata["metadata1"]);
            }

            [Test]
            public void AlwaysOverridesMetadata()
            {
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

                var m2 = new Metadata();
                m2["metadata2"] = "othermetadata";
                _store.SaveData(_location, new DataWithMetadata(data, m2)).Wait();

                var metadata = GetMetadata(_location);
                Assert.IsFalse(metadata.ContainsKey("metadata1"));
                Assert.AreEqual("othermetadata", metadata["metadata2"]);
            }

            private IDictionary<string, string> GetMetadata(StoreLocation location)
            {
                var resp = _client.GetObjectMetadata(new GetObjectMetadataRequest
                {
                    BucketName = _bucket,
                    Key = Path.Combine(location.Container, location.BasePath),
                });

                return resp.Metadata.Keys.ToDictionary(s => s.Replace("x-amz-meta-", string.Empty), s => resp.Metadata[s]);
            }
        }

        [TestFixture]
        public class GetMetadataMethod : AmazonStoreTests
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
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

                var result = _store.GetMetadata(_location).Result;

                Assert.AreEqual("1024", result[MetadataConstants.SizeMetadataKey]);
                Assert.IsTrue(result.ContainsKey(MetadataConstants.ModifiedMetadataKey));
                Assert.AreEqual("somemetadata", result["metadata1"]);
            }
        }

        [TestFixture]
        public class LoadDataMethod : AmazonStoreTests
        {
            [Test]
            public void MetadataIsTransferedWhenSelectingAStream()
            {
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                var m = new Metadata();
                m["metadata1"] = "metadata";
                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

                var result = _store.LoadData(_location).Result;

                Assert.AreEqual("metadata", result.Metadata["metadata1"]);
            }

            [Test]
            public void NoFileReturnsFalse()
            {
                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }
        }

        [TestFixture]
        public class FindSnapshotsMethod : AmazonStoreTests
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
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                var m = new Metadata();
                m["metadata1"] = "metadata";
                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();

                var snapshots = _store.FindSnapshots(_location).ToEnumerable();

                Assert.AreEqual(1, snapshots.Count());
            }

            [Test]
            public void SubItemBlobSnapshotsAreNotIncluded()
            {
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                _store.SaveData(_location, new DataWithMetadata(data)).Wait();

                AmazonTestsHelper.SetupBlob(_bucket, "kalix-leo-tests\\AzureStoreTests.testdata\\subitem.data");
                var location2 = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata\\subitem.data");

                _store.SaveData(location2, new DataWithMetadata(data)).Wait();

                var snapshots = _store.FindSnapshots(_location).ToEnumerable();

                Assert.AreEqual(1, snapshots.Count());
            }
        }

        [TestFixture]
        public class LoadDataMethodWithSnapshot : AmazonStoreTests
        {
            [Test]
            public void MetadataIsTransferedWhenSelectingAStream()
            {
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                var m = new Metadata();
                m["metadata1"] = "metadata";
                _store.SaveData(_location, new DataWithMetadata(data, m)).Wait();
                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

                var result = _store.LoadData(_location, shapshot).Result;

                Assert.AreEqual("metadata", result.Metadata["metadata1"]);
            }

            [Test]
            public void NoFileReturnsFalse()
            {
                // Had to find a valid version number!
                var result = _store.LoadData(_location, "ffwBujO.zXJtBw9dpKcV2WeJ3XhRwR2x").Result;
                Assert.IsNull(result);
            }
        }

        [TestFixture]
        public class SoftDeleteMethod : AmazonStoreTests
        {
            [Test]
            public void BlobThatDoesNotExistShouldNotThrowError()
            {
                _store.SoftDelete(_location).Wait();
            }

            [Test]
            public void BlobThatIsSoftDeletedShouldNotBeLoadable()
            {
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                _store.SaveData(_location, new DataWithMetadata(data)).Wait();

                _store.SoftDelete(_location).Wait();

                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void ShouldNotDeleteSnapshots()
            {
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                _store.SaveData(_location, new DataWithMetadata(data)).Wait();
                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

                _store.SoftDelete(_location).Wait();

                var result = _store.LoadData(_location, shapshot).Result;
                Assert.IsNotNull(result);
            }
        }

        [TestFixture]
        public class PermanentDeleteMethod : AmazonStoreTests
        {
            [Test]
            public void BlobThatDoesNotExistShouldNotThrowError()
            {
                _store.PermanentDelete(_location).Wait();
            }

            [Test]
            public void BlobThatIsSoftDeletedShouldNotBeLoadable()
            {
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                _store.SaveData(_location, new DataWithMetadata(data)).Wait();

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void ShouldDeleteAllSnapshots()
            {
                var data = Observable.Return(AmazonTestsHelper.RandomData(1));
                _store.SaveData(_location, new DataWithMetadata(data)).Wait();
                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location, shapshot).Result;
                Assert.IsNull(result);
            }
        }
    }
}
