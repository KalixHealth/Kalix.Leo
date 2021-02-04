using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Kalix.Leo.Azure.Storage;
using Kalix.Leo.Storage;
using NUnit.Framework;
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests.Storage
{
    [TestFixture]
    public class AzureStoreTests
    {
        protected AzureStore _store;
        protected BlockBlobClient _blob;
        protected StoreLocation _location;

        [SetUp]
        public virtual void Init()
        {
            _store = new AzureStore(AzureTestsHelper.GetDevelopentService(), true);

            _blob = AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata", true);
            _location = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata");
        }

        protected string WriteData(StoreLocation location, Metadata m, byte[] data)
        {
            var ct = CancellationToken.None;
            var res = _store.SaveData(location, m, null, async (s) =>
            {
                await s.WriteAsync(data.AsMemory(), ct);
                return data.Length;
            }, ct).Result;
            return res.Snapshot;
        }

        protected OptimisticStoreWriteResult TryOptimisticWrite(StoreLocation location, Metadata m, byte[] data)
        {
            var ct = CancellationToken.None;
            return _store.TryOptimisticWrite(location, m, null, async (s) =>
            {
                await s.WriteAsync(data.AsMemory(), ct);
                return data.Length;
            }, ct).Result;
        }

        [TestFixture]
        public class SaveDataMethod : AzureStoreTests
        {
            [Test]
            public void HasMetadataCorrectlySavesIt()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata
                {
                    ["metadata1"] = "somemetadata"
                };
                WriteData(_location, m, data);

                var props = _blob.GetProperties().Value;
                Assert.AreEqual("B64_c29tZW1ldGFkYXRh", props.Metadata["metadata1"]);
            }

            [Test]
            public void AlwaysOverridesMetadata()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata
                {
                    ["metadata1"] = "somemetadata"
                };
                WriteData(_location, m, data);

                var m2 = new Metadata
                {
                    ["metadata2"] = "othermetadata"
                };
                WriteData(_location, m2, data);

                var props = _blob.GetProperties().Value;
                Assert.IsFalse(props.Metadata.ContainsKey("metadata1"));
                Assert.AreEqual("B64_b3RoZXJtZXRhZGF0YQ==", props.Metadata["metadata2"]);
            }

            [Test]
            public void WritesStoreVersion()
            {
                var data = AzureTestsHelper.RandomData(1);
                WriteData(_location, null, data);

                var props = _blob.GetProperties().Value;
                Assert.AreEqual("2.0", props.Metadata["leoazureversion"]);
            }

            [Test]
            public void MultiUploadLargeFileIsSuccessful()
            {
                var data = AzureTestsHelper.RandomData(7);
                WriteData(_location, null, data);

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
                var m = new Metadata
                {
                    ["metadata1"] = "somemetadata"
                };
                var success = TryOptimisticWrite(_location, m, data);

                var props = _blob.GetProperties().Value;
                Assert.IsTrue(success.Result);
                Assert.IsNotNull(success.Metadata.Snapshot);
                Assert.AreEqual("B64_c29tZW1ldGFkYXRh", props.Metadata["metadata1"]);
            }

            [Test]
            public void AlwaysOverridesMetadata()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata
                {
                    ["metadata1"] = "somemetadata"
                };
                var success1 = TryOptimisticWrite(_location, m, data);
                var oldMetadata = _store.GetMetadata(_location).Result;

                var m2 = new Metadata
                {
                    ETag = oldMetadata.ETag,
                    ["metadata2"] = "othermetadata"
                };
                var success2 = TryOptimisticWrite(_location, m2, data);
                var newMetadata = _store.GetMetadata(_location).Result;

                var props = _blob.GetProperties().Value;
                Assert.IsTrue(success1.Result, "first write failed");
                Assert.IsTrue(success2.Result, "second write failed");
                Assert.AreEqual(success1.Metadata.Snapshot, oldMetadata.Snapshot);
                Assert.AreEqual(success2.Metadata.Snapshot, newMetadata.Snapshot);
                Assert.AreNotEqual(success1.Metadata.Snapshot, success2.Metadata.Snapshot);
                Assert.IsFalse(props.Metadata.ContainsKey("metadata1"));
                Assert.AreEqual("B64_b3RoZXJtZXRhZGF0YQ==", props.Metadata["metadata2"]);
            }

            [Test]
            public void WritesStoreVersion()
            {
                var data = AzureTestsHelper.RandomData(1);
                TryOptimisticWrite(_location, null, data);

                var props = _blob.GetProperties().Value;
                Assert.AreEqual("2.0", props.Metadata["leoazureversion"]);
            }

            [Test]
            public void NoETagMustBeNewSave()
            {
                var data = AzureTestsHelper.RandomData(1);
                var success1 = TryOptimisticWrite(_location, null, data);
                var success2 = TryOptimisticWrite(_location, null, data);

                Assert.IsTrue(success1.Result, "first write failed");
                Assert.IsFalse(success2.Result, "second write succeeded");
            }

            [Test]
            public void ETagDoesNotMatchFails()
            {
                var data = AzureTestsHelper.RandomData(1);
                var metadata = new Metadata { ETag = "notreal" };
                var success = TryOptimisticWrite(_location, metadata, data);

                Assert.IsFalse(success.Result, "write should not have succeeded with fake eTag");
            }

            [Test]
            public void MultiUploadLargeFileIsSuccessful()
            {
                var data = AzureTestsHelper.RandomData(7);
                var success = TryOptimisticWrite(_location, null, data);

                Assert.IsTrue(success.Result);
                Assert.IsNotNull(success.Metadata.Snapshot);
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
                var m = new Metadata
                {
                    ["metadata1"] = "somemetadata"
                };
                WriteData(_location, m, data);

                var result = _store.GetMetadata(_location).Result;

                Assert.AreEqual("1048576", result[MetadataConstants.ContentLengthMetadataKey]);
                Assert.IsTrue(result.ContainsKey(MetadataConstants.ModifiedMetadataKey));
                Assert.IsNotNull(result.Snapshot);
                Assert.AreEqual("somemetadata", result["metadata1"]);
            }

            [Test]
            public void DoesNotReturnInternalVersion()
            {
                var data = AzureTestsHelper.RandomData(1);
                WriteData(_location, null, data);

                var result = _store.GetMetadata(_location).Result;

                Assert.IsFalse(result.ContainsKey("leoazureversion"));
            }
        }

        [TestFixture]
        public class LoadDataMethod : AzureStoreTests
        {
            [Test]
            public async Task MetadataIsTransferedWhenSelectingAStream()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata
                {
                    ["metadata1"] = "metadata"
                };

                WriteData(_location, m, data);

                var result = await _store.LoadData(_location);
                Assert.IsNotNull(result.Metadata.Snapshot);
                Assert.AreEqual("metadata", result.Metadata["metadata1"]);
            }

            [Test]
            public async Task NoFileReturnsFalse()
            {
                var result = await _store.LoadData(_location);
                Assert.IsNull(result);
            }

            [Test]
            public async Task NoContainerReturnsFalse()
            {
                var result = await _store.LoadData(new StoreLocation("blahblahblah", "blah"));
                Assert.IsNull(result);
            }

            [Test]
            public void FileMarkedAsDeletedReturnsNull()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata
                {
                    ["leodeleted"] = DateTime.UtcNow.Ticks.ToString()
                };
                WriteData(_location, m, data);

                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public async Task AllDataLoadsCorrectly()
            {
                var data = AzureTestsHelper.RandomData(1);
                WriteData(_location, null, data);

                var result = _store.LoadData(_location).Result;
                byte[] resData;
                using(var ms = new MemoryStream())
                {
                    await result.Reader.CopyToAsync(ms, CancellationToken.None);
                    resData = ms.ToArray();
                }
                Assert.IsTrue(data.SequenceEqual(resData));
            }

            [Test]
            public async Task AllDataLargeFileLoadsCorrectly()
            {
                var data = AzureTestsHelper.RandomData(14);
                WriteData(_location, null, data);

                var result = _store.LoadData(_location).Result;
                byte[] resData;
                using (var ms = new MemoryStream())
                {
                    await result.Reader.CopyToAsync(ms, CancellationToken.None);
                    resData = ms.ToArray();
                }
                Assert.IsTrue(data.SequenceEqual(resData));
            }

            [Test]
            public void DoesNotReturnInternalVersion()
            {
                var data = AzureTestsHelper.RandomData(1);
                WriteData(_location, null, data);

                var result = _store.LoadData(_location).Result;

                Assert.IsFalse(result.Metadata.ContainsKey("leoazureversion"));
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
                var m = new Metadata
                {
                    ["metadata1"] = "metadata"
                };
                WriteData(_location, m, data);

                var snapshots = _store.FindSnapshots(_location).ToEnumerable();

                Assert.AreEqual(1, snapshots.Count());
            }

            [Test]
            public void SubItemBlobSnapshotsAreNotIncluded()
            {
                var data = AzureTestsHelper.RandomData(1);
                WriteData(_location, null, data);

                AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data", true);
                var location2 = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data");

                WriteData(location2, null, data);

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
                var m = new Metadata
                {
                    ["metadata1"] = "metadata"
                };
                var shapshot = WriteData(_location, m, data);

                var res = _store.LoadData(_location, shapshot).Result;
                Assert.AreEqual(shapshot, res.Metadata.Snapshot);
                Assert.AreEqual("metadata", res.Metadata["metadata1"]);
            }

            [Test]
            public void NoFileReturnsFalse()
            {
                var result = _store.LoadData(_location, DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture)).Result;
                Assert.IsNull(result);
            }
        }

        [TestFixture]
        public class SoftDeleteMethod : AzureStoreTests
        {
            [Test]
            public void BlobThatDoesNotExistShouldNotThrowError()
            {
                _store.SoftDelete(_location, null).Wait();
            }

            [Test]
            public void BlobThatIsSoftDeletedShouldNotBeLoadable()
            {
                var data = AzureTestsHelper.RandomData(1);
                WriteData(_location, null, data);

                _store.SoftDelete(_location, null).Wait();

                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void ShouldNotDeleteSnapshots()
            {
                var data = AzureTestsHelper.RandomData(1);
                WriteData(_location, null, data);
                var shapshot = _store.FindSnapshots(_location).ToEnumerable().Single().Id;

                _store.SoftDelete(_location, null).Wait();

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
                WriteData(_location, null, data);

                _store.PermanentDelete(_location).Wait();

                var result = _store.LoadData(_location).Result;
                Assert.IsNull(result);
            }

            [Test]
            public void ShouldDeleteAllSnapshots()
            {
                var data = AzureTestsHelper.RandomData(1);
                WriteData(_location, null, data);
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
            public async Task LockSuceedsEvenIfNoFile()
            {
                var l = _store.Lock(_location).Result;
                try
                {
                    Assert.IsNotNull(l);
                }
                finally
                {
                    await l.DisposeAsync();
                }
            }

            [Test]
            public async Task IfAlreadyLockedOtherLocksFail()
            {
                var l = await _store.Lock(_location);
                var l2 = await _store.Lock(_location);
                try
                {
                    Assert.IsNotNull(l);
                    Assert.IsNull(l2);
                }
                finally
                {
                    await l.DisposeAsync();
                }
            }

            [Test]
            public void IfFileLockedReturnsFalse()
            {
                Assert.ThrowsAsync<LockException>(async () =>
                {
                    var l = await _store.Lock(_location);
                    try
                    {
                        var data = AzureTestsHelper.RandomData(1);
                        try
                        {
                            WriteData(_location, null, data);
                        }
                        catch (AggregateException e)
                        {
                            throw e.InnerException;
                        }
                    }
                    finally
                    {
                        await l.DisposeAsync();
                    }
                });
            }
        }
    }
}
