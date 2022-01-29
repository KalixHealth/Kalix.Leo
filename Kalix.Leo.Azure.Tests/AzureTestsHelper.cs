using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Collections.Generic;

namespace Kalix.Leo.Azure.Tests
{
    public static class AzureTestsHelper
    {
        private const long KB = 1024;
        private const long MB = 1024 * KB;

        private static readonly Dictionary<string, BlobContainerClient> _containers = new();
        private static readonly Random _random = new();

        public static readonly string DevelopmetStorage = "UseDevelopmentStorage=true";

        public static BlobServiceClient GetBlobService()
        {
            // "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://ipv4.fiddler"
            return new BlobServiceClient(DevelopmetStorage);
        }

        public static BlobContainerClient GetContainer(string name)
        {
            if(!_containers.ContainsKey(name))
            {
                var client = GetBlobService();
                var container = client.GetBlobContainerClient(name);
                container.CreateIfNotExists();

                _containers[name] = container;
            }

            return _containers[name];
        }

        public static BlockBlobClient GetBlockBlob(string container, string path, bool del)
        {
            var c = GetContainer(container);
            var b = c.GetBlockBlobClient(path);
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

        public static TableServiceClient GetTableService()
        {
            return new TableServiceClient(DevelopmetStorage);
        }

        public static TableClient GetTable(string tableName)
        {
            return new TableClient(DevelopmetStorage, tableName);
        }
    }
}
