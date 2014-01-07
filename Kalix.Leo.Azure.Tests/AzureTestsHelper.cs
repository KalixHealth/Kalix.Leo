using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;

namespace Kalix.Leo.Azure.Tests
{
    public static class AzureTestsHelper
    {
        private const long KB = 1024;
        private const long MB = 1024 * KB;

        private static Dictionary<string, CloudBlobContainer> _containers = new Dictionary<string,CloudBlobContainer>();
        private static Random _random = new Random();

        public static CloudBlobContainer GetContainer(string name)
        {
            if(!_containers.ContainsKey(name))
            {
                var client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient();
                var container = client.GetContainerReference(name);
                container.CreateIfNotExists();

                _containers[name] = container;
            }

            return _containers[name];
        }

        public static CloudBlockBlob GetBlockBlob(string container, string path, bool del)
        {
            var c = GetContainer(container);
            var b = c.GetBlockBlobReference(path);
            if (del)
            {
                b.DeleteIfExists(DeleteSnapshotsOption.IncludeSnapshots);
            }

            return b;
        }

        public static byte[] RandomData(long noOfMb)
        {
            var data = new byte[noOfMb * MB];
            _random.NextBytes(data);
            return data;
        }
    }
}
