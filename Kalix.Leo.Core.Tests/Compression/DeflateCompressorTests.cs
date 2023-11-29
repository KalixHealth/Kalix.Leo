using Kalix.Leo.Compression;
using NUnit.Framework;
using System;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kalix.Leo.Core.Tests.Compression;

[TestFixture]
public class DeflateCompressorTests
{
    private const long KB = 1024;
    private const long MB = 1024 * KB;
    private static readonly Random _random = new();

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

        Assert.That(str, Is.EqualTo(decStr));
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

        Assert.That(data.SequenceEqual(newData));
    }

    [Test]
    public async Task SmallCanCompressAndDecompressUsingPipelines()
    {
        var str = "This is a string to compress and uncompress";
        var data = Encoding.UTF8.GetBytes(str);

        byte[] compData;
        using (var cms = new MemoryStream())
        {
            var writer = PipeWriter.Create(cms);
            using var compressed = _compressor.CompressWriteStream(writer.AsStream());
            var innerWriter = PipeWriter.Create(compressed);
            await innerWriter.WriteAsync(data.AsMemory());
            await innerWriter.CompleteAsync();
            compData = cms.ToArray();
        }

        byte[] decData;
        using (var rms = new MemoryStream(compData, false))
        {
            var reader = PipeReader.Create(rms);
            using var decompressed = _compressor.DecompressReadStream(reader.AsStream());
            var innerReader = PipeReader.Create(decompressed);
            using var ms = new MemoryStream();
            await innerReader.CopyToAsync(ms);
            decData = ms.ToArray();
        }

        var decStr = Encoding.UTF8.GetString(decData, 0, decData.Length);
        Assert.That(str, Is.EqualTo(decStr));
    }

    [Test]
    public async Task LargeCanCompressAndDecompressUsingPipelines()
    {
        var data = RandomData(1);

        byte[] compData;
        using (var cms = new MemoryStream())
        {
            var writer = PipeWriter.Create(cms);
            using var compressed = _compressor.CompressWriteStream(writer.AsStream());
            var innerWriter = PipeWriter.Create(compressed);
            await innerWriter.WriteAsync(data.AsMemory());
            await innerWriter.CompleteAsync();
            compData = cms.ToArray();
        }

        byte[] newData;
        using (var rms = new MemoryStream(compData, false))
        {
            var reader = PipeReader.Create(rms);
            using var decompressed = _compressor.DecompressReadStream(reader.AsStream());
            var innerReader = PipeReader.Create(decompressed);
            using var ms = new MemoryStream();
            await innerReader.CopyToAsync(ms);
            newData = ms.ToArray();
        }

        Assert.That(data.SequenceEqual(newData));
    }

    private static byte[] RandomData(long noOfMb)
    {
        var data = new byte[noOfMb * MB];
        _random.NextBytes(data);
        return data;
    }
}