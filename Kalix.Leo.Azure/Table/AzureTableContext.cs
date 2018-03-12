using Kalix.Leo.Encryption;
using Kalix.Leo.Table;
using Lokad.Cloud.Storage.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using E = Kalix.Leo.Table.ITableEntity;

namespace Kalix.Leo.Azure.Table
{
    public sealed class AzureTableContext : ITableContext
    {
        private readonly TableBatchOperation _context;
        private readonly TableBatchOperation _deleteBackupContext;
        private readonly CloudTable _table;
        private readonly IEncryptor _encryptor;
        private bool _hasSaved;
        private bool _isDirty;

        public AzureTableContext(CloudTable table, IEncryptor encryptor)
        {
            _table = table;
            _encryptor = encryptor;
            _context = new TableBatchOperation();
            _deleteBackupContext = new TableBatchOperation();
        }

        public void Replace(E entity)
        {
            CheckNotSaved();
            var fat = ConvertToFatEntity(entity);
            fat.ETag = fat.ETag ?? "*";
            _context.Replace(fat);
            _isDirty = true;
        }

        public void Insert(E entity)
        {
            CheckNotSaved();

            _context.Insert(ConvertToFatEntity(entity));
            _isDirty = true;
        }

        public void InsertOrMerge(E entity)
        {
            CheckNotSaved();

            _context.InsertOrMerge(ConvertToFatEntity(entity));
            _isDirty = true;
        }

        public void InsertOrReplace(E entity)
        {
            CheckNotSaved();

            _context.InsertOrReplace(ConvertToFatEntity(entity));
            _isDirty = true;
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
            _deleteBackupContext.InsertOrReplace(fat);
            _isDirty = true;
        }

        // Since this context is limited to a specific table... can always do batch requests...
        public async Task Save()
        {
            CheckNotSaved();

            if (_isDirty)
            {
                try
                {
                    bool tryWithDeletes = true;

                    // If you try to delete a row that doesn't exist then you get a resource not found exception
                    try
                    {
                        await _table.ExecuteBatchAsync(_context).ConfigureAwait(false);
                        tryWithDeletes = false;
                    }
                    catch(StorageException ex)
                    {
                        if (ex.RequestInformation.HttpStatusCode != 404)
                        {
                            throw;
                        }
                    }

                    // In that case, we want to try and add the deleted items,
                    if(tryWithDeletes)
                    {
                        await _table.ExecuteBatchAsync(_deleteBackupContext).ConfigureAwait(false);
                        await _table.ExecuteBatchAsync(_context).ConfigureAwait(false);
                    }
                }
                catch (StorageException ex)
                {
                    if (ex.RequestInformation?.ErrorCode == "EntityAlreadyExists" || (ex.RequestInformation?.ExtendedErrorInformation?.ErrorMessage.Contains("The specified entity already exists") ?? false))
                    {
                        throw new StorageEntityAlreadyExistsException(ex.RequestInformation.ExtendedErrorInformation.ErrorMessage, ex);
                    }

                    throw ex.Wrap("Table: " + _table.Name);
                }

                _hasSaved = true;
            }
        }

        private void CheckNotSaved()
        {
            if (_hasSaved)
            {
                throw new InvalidOperationException("Can only save context once... create another one!");
            }
        }

        private FatEntity ConvertToFatEntity(E entity)
        {
            byte[] data;
            if (entity.DataObject == null)
            {
                data = new byte[0];
            }
            else
            {
                data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(entity.DataObject));

                if (_encryptor != null)
                {
                    using(var ms = new MemoryStream())
                    {
                        using(var enc = _encryptor.Encrypt(ms, false))
                        {
                            enc.Write(data, 0, data.Length);
                        }

                        data = ms.ToArray();
                    }
                }
            }

            var fat = new FatEntity()
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey
            };

            fat.SetData(data, data.Length);
            return fat;
        }
    }
}
