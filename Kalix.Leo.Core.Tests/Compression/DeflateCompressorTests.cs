using Kalix.Leo.Compression;
using NUnit.Framework;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reactive.Linq;
using System.Text;

namespace Kalix.Leo.Core.Tests.Compression
{
    [TestFixture]
    public class DeflateCompressorTests
    {
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

            var compressed = _compressor.Compress(Observable.Return(data));
            var decompressed = _compressor.Decompress(compressed);
            
            var decData = decompressed.ToEnumerable().SelectMany(b => b).ToArray();
            var decStr = Encoding.UTF8.GetString(decData, 0, decData.Length);

            Assert.AreEqual(str, decStr);
        }

        [Test]
        public void SmallPureCompressAndDecompress()
        {
            var str = "This is a string to compress and uncompress";
            var data = Encoding.UTF8.GetBytes(str);

            byte[] compressed;

            using (var ms = new MemoryStream())
            {
                using (var stream = new GZipStream(ms, CompressionMode.Compress))
                {
                    stream.Write(data, 0, data.Length);
                }
                compressed = ms.ToArray();
            }
            

            byte[] newData = new byte[data.Length];
            using(var ms = new MemoryStream(compressed))
            using(var stream = new GZipStream(ms, CompressionMode.Decompress))
            {
                stream.Read(newData, 0, data.Length);
            }

            var decStr = Encoding.UTF8.GetString(newData, 0, newData.Length);

            Assert.AreEqual(str, decStr);
        }

        [Test]
        public void LargeCanCompressAndDecompress()
        {
            var random = new Random();
            var data = Observable.Generate(0, i => i < 1000, i => ++i, i =>
            {
                var bytes = new byte[1024];
                random.NextBytes(bytes);
                return bytes;
            });

            var compressed = _compressor.Compress(data);
            var decompressed = _compressor.Decompress(compressed);

            decompressed.Wait();
        }

        [Test]
        public void LargePureCompressAndDecompress()
        {
            var random = new Random();
            var data = Observable.Generate(0, i => i < 1000, i => ++i, i =>
            {
                var bytes = new byte[1024];
                random.NextBytes(bytes);
                return bytes;
            });

            byte[] compressed;
            using (var ms = new MemoryStream())
            {
                using (var stream = new GZipStream(ms, CompressionMode.Compress))
                {
                    data
                        .Do(bytes => stream.Write(bytes, 0, bytes.Length))
                        .Wait();
                }
                compressed = ms.ToArray();
            }


            byte[] newData = new byte[1024];
            using (var ms = new MemoryStream(compressed))
            using (var stream = new GZipStream(ms, CompressionMode.Decompress))
            {
                while (stream.Read(newData, 0, newData.Length) != 0) { };
            }
        }
    }
}
