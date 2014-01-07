using Amazon.S3;
using Amazon.S3.Model;
using Kalix.Leo.Amazon.Storage;
using NUnit.Framework;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading;

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
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string>() { { "metadata1", "somemetadata" } }).Wait();

                var metadata = GetMetadata(_location);
                Assert.AreEqual("somemetadata", metadata["metadata1"]);
            }

            [Test]
            public void AlwaysOverridesMetadata()
            {
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string>() { { "metadata1", "somemetadata" } }).Wait();
                
                data.Position = 0;
                _store.SaveData(data, _location, new Dictionary<string, string>() { { "metadata2", "othermetadata" } }).Wait();

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
        public class LoadDataMethod : AmazonStoreTests
        {
            [Test]
            public void NullStreamCancelsTheDownload()
            {
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();

                var hasFile = _store.LoadData(_location, m => null).Result;
                Assert.IsFalse(hasFile);
            }

            [Test]
            public void MetadataIsTransferedWhenSelectingAStream()
            {
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
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
        }

        [TestFixture]
        public class FindSnapshotsMethod : AmazonStoreTests
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
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location, new Dictionary<string, string> { { "metadata1", "metadata" } }).Wait();

                var snapshots = _store.FindSnapshots(_location).Result;

                Assert.AreEqual(1, snapshots.Count());
            }

            [Test]
            public void SubItemBlobSnapshotsAreNotIncluded()
            {
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();

                AmazonTestsHelper.SetupBlob(_bucket, "kalix-leo-tests\\AzureStoreTests.testdata\\subitem.data");
                var location2 = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata\\subitem.data");
                data.Position = 0;

                _store.SaveData(data, location2).Wait();

                var snapshots = _store.FindSnapshots(_location).Result;

                Assert.AreEqual(1, snapshots.Count());
            }

            [Test]
            public void SnapshotsAreFromNewestToOldest()
            {
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                Thread.Sleep(1000);
                _store.SaveData(data, _location).Wait();

                var snapshots = _store.FindSnapshots(_location).Result;

                var snapshot = snapshots.First().Modified;
                var snapshot2 = snapshots.Last().Modified;
                Assert.Less(snapshot2, snapshot);
            }
        }

        [TestFixture]
        public class LoadDataMethodWithSnapshot : AmazonStoreTests
        {
            [Test]
            public void NullStreamCancelsTheDownload()
            {
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                var shapshot = _store.FindSnapshots(_location).Result.Single().Id;

                var hasFile = _store.LoadData(_location, m => null, shapshot).Result;
                Assert.IsFalse(hasFile);
            }

            [Test]
            public void MetadataIsTransferedWhenSelectingAStream()
            {
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
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
                // Had to find a valid version number!
                var result = _store.LoadData(_location, m => null, "ffwBujO.zXJtBw9dpKcV2WeJ3XhRwR2x").Result;
                Assert.IsFalse(result);
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
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();

                _store.SoftDelete(_location).Wait();

                var result = _store.LoadData(_location, m => null).Result;
                Assert.IsFalse(result);
            }

            [Test]
            public void ShouldNotDeleteSnapshots()
            {
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                var shapshot = _store.FindSnapshots(_location).Result.Single().Id;

                _store.SoftDelete(_location).Wait();

                var result = _store.LoadData(_location, m => new MemoryStream(), shapshot).Result;
                Assert.IsTrue(result);
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
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location, m => null).Result;
                Assert.IsFalse(result);
            }

            [Test]
            public void ShouldDeleteAllSnapshots()
            {
                var data = new MemoryStream(AmazonTestsHelper.RandomData(1));
                _store.SaveData(data, _location).Wait();
                var shapshot = _store.FindSnapshots(_location).Result.Single().Id;

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location, m => new MemoryStream(), shapshot).Result;
                Assert.IsFalse(result);
            }
        }
    }
}
