using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Storage
{
    /// <summary>
    /// This is here just so that we support old streams
    /// </summary>
    public class AzureBlockBlobStream : Stream
    {
        private readonly CloudBlockBlob _blob;
        private byte[] _data;

        private int _position;

        public AzureBlockBlobStream(CloudBlockBlob blob)
        {
            _blob = blob;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            if (_data == null)
            {
                using (var ms = new MemoryStream())
                {
                    var blockList = (await _blob.DownloadBlockListAsync().ConfigureAwait(false)).ToList();
                    if (blockList.Any())
                    {
                        // Make sure that we get the blocks in order...
                        var orderedList = blockList.OrderBy(l => BitConverter.ToInt32(Convert.FromBase64String(l.Name), 0)).Select(o => Tuple.Create(blockList.IndexOf(o), o)).ToList();
                        foreach (var o in orderedList)
                        {
                            // Do not assume each block is uniform... Make sure to use length info of previous blocks
                            var start = orderedList.Where(ol => ol.Item1 < o.Item1).Sum(ol => ol.Item2.Length);
                            await _blob.DownloadRangeToStreamAsync(ms, start, o.Item2.Length, ct).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        await _blob.DownloadToStreamAsync(ms, ct).ConfigureAwait(false);
                    }

                    _data = ms.ToArray();
                }
            }

            var length = Math.Min(_data.Length - _position, count);
            if (length > 0)
            {
                Buffer.BlockCopy(_data, _position, buffer, offset, length);
                _position += length;
            }

            return length;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_data == null)
            {
                using (var ms = new MemoryStream())
                {
                    var blockList = _blob.DownloadBlockList().ToList();
                    if (blockList.Any())
                    {
                        // Make sure that we get the blocks in order...
                        var orderedList = blockList.OrderBy(l => BitConverter.ToInt32(Convert.FromBase64String(l.Name), 0)).Select(o => Tuple.Create(blockList.IndexOf(o), o)).ToList();
                        foreach (var o in orderedList)
                        {
                            // Do not assume each block is uniform... Make sure to use length info of previous blocks
                            var start = orderedList.Where(ol => ol.Item1 < o.Item1).Sum(ol => ol.Item2.Length);
                            _blob.DownloadRangeToStream(ms, start, o.Item2.Length);
                        }
                    }
                    else
                    {
                        _blob.DownloadToStreamAsync(ms);
                    }

                    _data = ms.ToArray();
                }
            }

            var length = Math.Min(_data.Length - _position, count);
            if (length > 0)
            {
                Buffer.BlockCopy(_data, _position, buffer, offset, length);
                _position += length;
            }

            return length;
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override long Length
        {
            get { throw new NotImplementedException(); }
        }

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
