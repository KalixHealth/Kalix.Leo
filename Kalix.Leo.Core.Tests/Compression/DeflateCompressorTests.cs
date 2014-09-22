using Kalix.Leo.Compression;
using NUnit.Framework;
using System;
using System.IO;
using System.Linq;
using System.Text;

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

            var compressed = _compressor.Compress(new MemoryStream(data), true);
            var decompressed = _compressor.Decompress(compressed, true);
            
            byte[] decData;
            using(var ms = new MemoryStream())
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
            using(var cms = new MemoryStream())
            using (var compressed = _compressor.Compress(new MemoryStream(data), true))
            {
                compressed.CopyTo(cms);
                compData = cms.ToArray();
            }

            var decompressed = _compressor.Decompress(new MemoryStream(compData), true);

            byte[] newData;
            using(var ms = new MemoryStream())
            {
                decompressed.CopyTo(ms);
                newData = ms.ToArray();
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
