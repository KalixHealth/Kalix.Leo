using Microsoft.Experience.CloudFx.Framework.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure
{
    public class AzureStore : IStore
    {
        public AzureStore(ICloudBlobStorage blobStorage)
        {
        }

        public Task SaveData(Stream data, StoreLocation location)
        {
            throw new NotImplementedException();
        }

        public Task<System.IO.Stream> LoadData(StoreLocation location)
        {
            throw new NotImplementedException();
        }

        public Task<DateTime> TakeSnapshot(StoreLocation location)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<DateTime>> FindAllSnapshots(DateTime? earilest = null, DateTime? latest = null, int? take = 10)
        {
            throw new NotImplementedException();
        }

        public Task<System.IO.Stream> LoadSnapshotData(StoreLocation location, DateTime snapshot)
        {
            throw new NotImplementedException();
        }

        public Task SoftDelete(StoreLocation location)
        {
            throw new NotImplementedException();
        }

        public Task PermanentDelete(StoreLocation location)
        {
            throw new NotImplementedException();
        }
    }
}
