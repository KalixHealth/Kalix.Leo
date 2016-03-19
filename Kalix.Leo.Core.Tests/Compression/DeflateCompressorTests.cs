using Kalix.Leo.Compression;
using Kalix.Leo.Streams;
using NUnit.Framework;
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Kalix.Leo.Core.Tests.Compression
{
    [TestFixture]
    public class DeflateCompressorTests
    {
        private const long KB = 1024;
        private const long MB = 1024 * KB;
        private static Random _random = new Random();

        protected DeflateCompressor _compressor;

        [SetUp]
        public void Init()
        {
            _compressor = new DeflateCompressor();
        }

        [Test]
        public void SmallCanCompressAndDecompress()
        {
            var str = "This is a string to compress and uncompress";
            var data = Encoding.UTF8.GetBytes(str);

            byte[] compData;
            using (var cms = new MemoryStream())
            {
                using (var compressed = _compressor.CompressWriteStream(cms))
                {
                    compressed.Write(data, 0, data.Length);
                }
                compData = cms.ToArray();
            }

            byte[] decData;
            using (var ms = new MemoryStream())
            using (var decompressed = _compressor.DecompressReadStream(new MemoryStream(compData)))
            {
                decompressed.CopyTo(ms);
                decData = ms.ToArray();
            }
            var decStr = Encoding.UTF8.GetString(decData, 0, decData.Length);

            Assert.AreEqual(str, decStr);
        }

        [Test]
        public void LargeCanCompressAndDecompress()
        {
            var data = RandomData(1);

            byte[] compData;
            using (var cms = new MemoryStream())
            {
                using (var compressed = _compressor.CompressWriteStream(cms))
                {
                    compressed.Write(data, 0, data.Length);
                }
                compData = cms.ToArray();
            }
            
            byte[] newData;
            using (var ms = new MemoryStream())
            using (var decompressed = _compressor.DecompressReadStream(new MemoryStream(compData)))
            {
                decompressed.CopyTo(ms);
                newData = ms.ToArray();
            }

            Assert.IsTrue(data.SequenceEqual(newData));
        }

        [Test]
        public void SmallCanCompressAndDecompressUsingStreamTransformer()
        {
            var str = "This is a string to compress and uncompress";
            var data = Encoding.UTF8.GetBytes(str);

            byte[] compData;
            using (var cms = new AsyncMemoryStream())
            using (var compressed = cms.AddTransformer(_compressor.CompressWriteStream))
            {
                compressed.WriteAsync(data, 0, data.Length, CancellationToken.None).Wait();
                compressed.Complete(CancellationToken.None).Wait();

                compData = cms.ToArray();
            }

            byte[] decData;
            using (var reader = new ReadStreamWrapper(new MemoryStream(compData)))
            using (var decompressed = reader.AddTransformer(_compressor.DecompressReadStream))
            {
                decData = decompressed.ReadBytes().Result;
            }

            var decStr = Encoding.UTF8.GetString(decData, 0, decData.Length);
            Assert.AreEqual(str, decStr);
        }

        [Test]
        public void LargeCanCompressAndDecompressUsingStreamTransformer()
        {
            var data = RandomData(1);

            byte[] compData;
            using (var cms = new AsyncMemoryStream())
            using (var compressed = cms.AddTransformer(_compressor.CompressWriteStream))
            {
                compressed.WriteAsync(data, 0, data.Length, CancellationToken.None).Wait();
                compressed.Complete(CancellationToken.None).Wait();

                compData = cms.ToArray();
            }

            byte[] newData;
            using (var reader = new ReadStreamWrapper(new MemoryStream(compData)))
            using (var decompressed = reader.AddTransformer(_compressor.DecompressReadStream))
            {
                newData = decompressed.ReadBytes().Result;
            }

            Assert.IsTrue(data.SequenceEqual(newData));
        }

        private static byte[] RandomData(long noOfMb)
        {
            var data = new byte[noOfMb * MB];
            _random.NextBytes(data);
            return data;
        }
    }
}
