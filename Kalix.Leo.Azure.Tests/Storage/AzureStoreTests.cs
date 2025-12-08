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

namespace Kalix.Leo.Azure.Tests.Storage;

[TestFixture]
public class AzureStoreTests
{
    protected AzureStore _store;
    protected BlockBlobClient _blob;
    protected StoreLocation _location;

    [SetUp]
    public virtual void Init()
    {
        _store = new AzureStore(AzureTestsHelper.GetBlobService(), true);

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
            Assert.That("B64_c29tZW1ldGFkYXRh", Is.EqualTo(props.Metadata["metadata1"]));
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
            Assert.That(!props.Metadata.ContainsKey("metadata1"));
            Assert.That("B64_b3RoZXJtZXRhZGF0YQ==", Is.EqualTo(props.Metadata["metadata2"]));
        }

        [Test]
        public void WritesStoreVersion()
        {
            var data = AzureTestsHelper.RandomData(1);
            WriteData(_location, null, data);

            var props = _blob.GetProperties().Value;
            Assert.That("2.0", Is.EqualTo(props.Metadata["leoazureversion"]));
        }

        [Test]
        public void MultiUploadLargeFileIsSuccessful()
        {
            var data = AzureTestsHelper.RandomData(7);
            WriteData(_location, null, data);

            Assert.That(_blob.Exists());
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
            Assert.That(success.Result);
            Assert.That(success.Metadata.Snapshot, Is.Not.Null);
            Assert.That("B64_c29tZW1ldGFkYXRh", Is.EqualTo(props.Metadata["metadata1"]));
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
            Assert.That(success1.Result, "first write failed");
            Assert.That(success2.Result, "second write failed");
            Assert.That(success1.Metadata.Snapshot, Is.EqualTo(oldMetadata.Snapshot));
            Assert.That(success2.Metadata.Snapshot, Is.EqualTo(newMetadata.Snapshot));
            Assert.That(success1.Metadata.Snapshot, Is.Not.EqualTo(success2.Metadata.Snapshot));
            Assert.That(!props.Metadata.ContainsKey("metadata1"));
            Assert.That("B64_b3RoZXJtZXRhZGF0YQ==", Is.EqualTo(props.Metadata["metadata2"]));
        }

        [Test]
        public void WritesStoreVersion()
        {
            var data = AzureTestsHelper.RandomData(1);
            TryOptimisticWrite(_location, null, data);

            var props = _blob.GetProperties().Value;
            Assert.That("2.0", Is.EqualTo(props.Metadata["leoazureversion"]));
        }

        [Test]
        public void NoETagMustBeNewSave()
        {
            var data = AzureTestsHelper.RandomData(1);
            var success1 = TryOptimisticWrite(_location, null, data);
            var success2 = TryOptimisticWrite(_location, null, data);

            Assert.That(success1.Result, "first write failed");
            Assert.That(!success2.Result, "second write succeeded");
        }

        [Test]
        public void ETagDoesNotMatchFails()
        {
            var data = AzureTestsHelper.RandomData(1);
            var metadata = new Metadata { ETag = "notreal" };
            var success = TryOptimisticWrite(_location, metadata, data);

            Assert.That(!success.Result, "write should not have succeeded with fake eTag");
        }

        [Test]
        public void MultiUploadLargeFileIsSuccessful()
        {
            var data = AzureTestsHelper.RandomData(7);
            var success = TryOptimisticWrite(_location, null, data);

            Assert.That(success.Result);
            Assert.That(success.Metadata.Snapshot, Is.Not.Null);
            Assert.That(_blob.Exists());
        }
    }

    [TestFixture]
    public class GetMetadataMethod : AzureStoreTests
    {
        [Test]
        public void NoFileReturnsNull()
        {
            var result = _store.GetMetadata(_location).Result;
            Assert.That(result, Is.Null);
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

            Assert.That("1048576", Is.EqualTo(result[MetadataConstants.ContentLengthMetadataKey]));
            Assert.That(result.ContainsKey(MetadataConstants.ModifiedMetadataKey));
            Assert.That(result.Snapshot, Is.Not.Null);
            Assert.That("somemetadata", Is.EqualTo(result["metadata1"]));
        }

        [Test]
        public void DoesNotReturnInternalVersion()
        {
            var data = AzureTestsHelper.RandomData(1);
            WriteData(_location, null, data);

            var result = _store.GetMetadata(_location).Result;

            Assert.That(!result.ContainsKey("leoazureversion"));
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
            Assert.That(result.Metadata.Snapshot, Is.Not.Null);
            Assert.That("metadata", Is.EqualTo(result.Metadata["metadata1"]));
        }

        [Test]
        public async Task NoFileReturnsFalse()
        {
            var result = await _store.LoadData(_location);
            Assert.That(result, Is.Null);
        }

        [Test]
        public async Task NoContainerReturnsFalse()
        {
            var result = await _store.LoadData(new StoreLocation("blahblahblah", "blah"));
            Assert.That(result, Is.Null);
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
            Assert.That(result, Is.Null);
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
            Assert.That(data.SequenceEqual(resData));
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
            Assert.That(data.SequenceEqual(resData));
        }

        [Test]
        public void DoesNotReturnInternalVersion()
        {
            var data = AzureTestsHelper.RandomData(1);
            WriteData(_location, null, data);

            var result = _store.LoadData(_location).Result;

            Assert.That(!result.Metadata.ContainsKey("leoazureversion"));
        }
    }

    [TestFixture]
    public class FindSnapshotsMethod : AzureStoreTests
    {
        [Test]
        public async Task NoSnapshotsReturnsEmpty()
        {
            var snapshots = await _store.FindSnapshots(_location).ToListAsync();

            Assert.That(0, Is.EqualTo(snapshots.Count()));
        }

        [Test]
        public async Task SingleSnapshotCanBeFound()
        {
            var data = AzureTestsHelper.RandomData(1);
            var m = new Metadata
            {
                ["metadata1"] = "metadata"
            };
            WriteData(_location, m, data);

            var snapshots = await _store.FindSnapshots(_location).ToListAsync();

            Assert.That(1, Is.EqualTo(snapshots.Count()));
        }

        [Test]
        public async Task SubItemBlobSnapshotsAreNotIncluded()
        {
            var data = AzureTestsHelper.RandomData(1);
            WriteData(_location, null, data);

            AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data", true);
            var location2 = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data");

            WriteData(location2, null, data);

            var snapshots = await _store.FindSnapshots(_location).ToListAsync();

            Assert.That(1, Is.EqualTo(snapshots.Count()));
        }
    }

    [TestFixture]
    public class FindFilesMethod : AzureStoreTests
    {
        [Test]
        public async Task NoContainerReturnsEmpty()
        {
            var files = await _store.FindFiles("not-exists").ToListAsync();
            Assert.That(0, Is.EqualTo(files.Count));
        }

        [Test]
        public async Task ContainerWithFilesReturnsCorrectNumber()
        {
            var files = await _store.FindFiles(_location.Container).ToListAsync();
            Assert.That(1, Is.EqualTo(files.Count));
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
            Assert.That(shapshot, Is.EqualTo(res.Metadata.Snapshot));
            Assert.That("metadata", Is.EqualTo(res.Metadata["metadata1"]));
        }

        [Test]
        public void NoFileReturnsFalse()
        {
            var result = _store.LoadData(_location, DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture)).Result;
            Assert.That(result, Is.Null);
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
            Assert.That(result, Is.Null);
        }

        [Test]
        public async Task ShouldNotDeleteSnapshots()
        {
            var data = AzureTestsHelper.RandomData(1);
            WriteData(_location, null, data);
            var shapshot = (await _store.FindSnapshots(_location).SingleAsync()).Id;

            _store.SoftDelete(_location, null).Wait();

            var result = _store.LoadData(_location, shapshot).Result;
            Assert.That(result, Is.Not.Null);
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
            Assert.That(result, Is.Null);
        }

        [Test]
        public async Task ShouldDeleteAllSnapshots()
        {
            var data = AzureTestsHelper.RandomData(1);
            WriteData(_location, null, data);
            var shapshot = (await _store.FindSnapshots(_location).SingleAsync()).Id;

            _store.PermanentDelete(_location).Wait();

            var result = _store.LoadData(_location, shapshot).Result;
            Assert.That(result, Is.Null);
        }
    }

    [TestFixture]
    public class LockMethod : AzureStoreTests
    {
        [Test]
        public async Task LockSuceedsEvenIfNoFile()
        {
            var l = await _store.Lock(_location);
            try
            {
                Assert.That(l, Is.Not.Null);
            }
            finally
            {
                await l.CancelDispose.DisposeAsync();
            }
        }

        [Test]
        public async Task IfAlreadyLockedOtherLocksFail()
        {
            var l = await _store.Lock(_location);
            var l2 = await _store.Lock(_location);
            try
            {
                Assert.That(l.CancelDispose, Is.Not.Null);
                Assert.That(l2.CancelDispose, Is.Null);
            }
            finally
            {
                await l.CancelDispose.DisposeAsync();
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
                    await l.CancelDispose.DisposeAsync();
                }
            });
        }
    }
}