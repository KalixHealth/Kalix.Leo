using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;

namespace Kalix.Leo.Amazon.Tests
{
    public static class AmazonTestsHelper
    {
        private const long KB = 1024;

        private static List<string> _containers = new List<string>();
        private static AmazonS3Client _client;
        private static Random _random = new Random();

        static AmazonTestsHelper()
        {
            var systemName = ConfigurationManager.AppSettings["RegionSystemName"];
            var region = RegionEndpoint.GetBySystemName(systemName);
            _client = new AmazonS3Client(region);
        }

        public static AmazonS3Client GetContainer(string name)
        {
            if(!_containers.Contains(name))
            {
                var request = new PutBucketRequest
                {
                    BucketName = name,
                    CannedACL = S3CannedACL.Private,
                    UseClientRegion = true
                };

                var resp = _client.PutBucket(request);

                _containers.Add(name);
            }

            return _client;
        }

        public static AmazonS3Client SetupBlob(string container, string path)
        {
            GetContainer(container);

            var request = new ListVersionsRequest
            {
                BucketName = container,
                Prefix = path
            };

            var resp = _client.ListVersions(request);
            var toDelete = resp.Versions.Where(v => v.Key == path).Select(v => new KeyVersion
            {
                Key = v.Key,
                VersionId = v.VersionId
            }).ToList();

            if (toDelete.Any())
            {
                var delRequest = new DeleteObjectsRequest
                {
                    BucketName = container,
                    Objects = toDelete,
                    Quiet = true
                };

                _client.DeleteObjects(delRequest);
            }

            return _client;
        }

        public static byte[] RandomData(long noOfKb)
        {
            var data = new byte[noOfKb * KB];
            _random.NextBytes(data);
            return data;
        }
    }
}
