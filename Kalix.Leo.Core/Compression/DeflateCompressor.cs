using Ionic.Zlib;
using Kalix.Leo.Compression;
using System;
using System.IO;
using System.Reactive.Linq;

namespace Kalix.Leo.Compression
{
    public class DeflateCompressor : ICompressor
    {
        // http://code.logos.com/blog/2012/06/always-wrap-gzipstream-with-bufferedstream.html
        private const int _optimalBuffer = 8192;

        public string Algorithm
        {
            get { return "deflate"; }
        }

        public IObservable<byte[]> Compress(IObservable<byte[]> data)
        {
            return Observable.Create<byte[]>(async (obs, ct) =>
            {
                await obs.UseWriteStream(async stream =>
                {
                    using (var zipStream = new DeflateStream(stream, CompressionMode.Compress))
                    {
                        await data
                            .BufferBytes(_optimalBuffer, false)
                            .Do(b => zipStream.Write(b, 0, b.Length));
                    }
                });
            });
        }

        public IObservable<byte[]> Decompress(IObservable<byte[]> compressedData)
        {
            return Observable.Create<byte[]>(async (obs, ct) =>
            {
                await obs.UseWriteStream(async stream =>
                {
                    using (var zipStream = new DeflateStream(stream, CompressionMode.Decompress))
                    {
                        await compressedData
                            .BufferBytes(_optimalBuffer, false)
                            .Do(b => zipStream.Write(b, 0, b.Length));
                    }
                });
            });
        }
    }
}
