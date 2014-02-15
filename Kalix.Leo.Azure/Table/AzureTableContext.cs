using Kalix.Leo.Encryption;
using Kalix.Leo.Table;
using Lokad.Cloud.Storage.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using E = Kalix.Leo.Table.ITableEntity;

namespace Kalix.Leo.Azure.Table
{
    public sealed class AzureTableContext : ITableContext
    {
        private readonly TableBatchOperation _context;
        private readonly CloudTable _table;
        private readonly IEncryptor _encryptor;
        private bool _hasSaved;

        public AzureTableContext(CloudTable table, IEncryptor encryptor)
        {
            _table = table;
            _encryptor = encryptor;
            _context = new TableBatchOperation();
        }

        #region ITableContext Members

        public void Replace(E entity)
        {
            CheckNotSaved();
            var fat = ConvertToFatEntity(entity);
            fat.ETag = fat.ETag ?? "*";
            _context.Replace(fat);
        }

        public void Insert(E entity)
        {
            CheckNotSaved();

            _context.Insert(ConvertToFatEntity(entity));
        }

        public void InsertOrMerge(E entity)
        {
            CheckNotSaved();

            _context.InsertOrMerge(ConvertToFatEntity(entity));
        }

        public void InsertOrReplace(E entity)
        {
            CheckNotSaved();

            _context.InsertOrReplace(ConvertToFatEntity(entity));
        }

        public void Delete(E entity)
        {
            CheckNotSaved();

            // Delete is special in that we do not need the data to delete
            var fat = new FatEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = "*"
            };
            _context.Delete(fat);
        }

        private FatEntity ConvertToFatEntity(E entity)
        {
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(entity.DataObject));

            if (_encryptor != null)
            {
                var encrypted = _encryptor.Encrypt(Observable.Return(data)).ToEnumerable();
                data = new byte[encrypted.Sum(d => d.LongLength)];
                int offset = 0;
                foreach (var d in encrypted)
                {
                    Buffer.BlockCopy(d, 0, data, offset, d.Length);
                    offset += d.Length;
                }
            }

            var fat = new FatEntity()
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey
            };

            fat.SetData(data);
            return fat;
        }

        // Since this context is limited to a specific table... can always do batch requests...
        public async Task Save()
        {
            CheckNotSaved();

            try
            {
                await _table.ExecuteBatchAsync(_context).ConfigureAwait(false);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.ExtendedErrorInformation.ErrorCode == "EntityAlreadyExists")
                {
                    throw new StorageEntityAlreadyExistsException(ex.RequestInformation.ExtendedErrorInformation.ErrorMessage, ex);
                }

                // Throw an error with more details...
                var extraData = JsonConvert.SerializeObject(ex.RequestInformation.ExtendedErrorInformation);
                throw new Exception("Storage Exception occured with additional details: " + extraData, ex);
            }

            _hasSaved = true;
        }

        #endregion

        private void CheckNotSaved()
        {
            if (_hasSaved)
            {
                throw new InvalidOperationException("Can only save context once... create another one!");
            }
        }
    }
}
