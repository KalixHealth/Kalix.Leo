using Azure;
using Azure.Data.Tables;
using Kalix.Leo.Encryption;
using Kalix.Leo.Table;
using Lokad.Cloud.Storage.Azure;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using E = Kalix.Leo.Table.ITableEntity;

namespace Kalix.Leo.Azure.Table;

public sealed class AzureTableContext : ITableContext
{
    private readonly List<TableTransactionAction> _context;
    private readonly List<TableTransactionAction> _deleteBackupContext;
    private readonly TableClient _table;
    private readonly IEncryptor _encryptor;
    private bool _hasSaved;
    private bool _isDirty;

    public AzureTableContext(TableClient table, IEncryptor encryptor)
    {
        _table = table;
        _encryptor = encryptor;
        _context = new List<TableTransactionAction>();
        _deleteBackupContext = new List<TableTransactionAction>();
    }

    public void Replace(E entity)
    {
        CheckNotSaved();
        var fat = ConvertToFatEntity(entity);
        fat.ETag = ETag.All;
        _context.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace, fat));
        _isDirty = true;
    }

    public void Insert(E entity)
    {
        CheckNotSaved();

        _context.Add(new TableTransactionAction(TableTransactionActionType.Add, ConvertToFatEntity(entity)));
        _isDirty = true;
    }

    public void InsertOrMerge(E entity)
    {
        CheckNotSaved();

        _context.Add(new TableTransactionAction(TableTransactionActionType.UpsertMerge, ConvertToFatEntity(entity)));
        _isDirty = true;
    }

    public void InsertOrReplace(E entity)
    {
        CheckNotSaved();

        _context.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, ConvertToFatEntity(entity)));
        _isDirty = true;
    }

    public void Delete(E entity)
    {
        CheckNotSaved();

        // Delete is special in that we do not need the data to delete
        var tb = new TableEntity(entity.PartitionKey, entity.RowKey)
        {
            ETag = ETag.All
        };
        _context.Add(new TableTransactionAction(TableTransactionActionType.Delete, tb));
        _deleteBackupContext.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, tb));
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
                    await _table.SubmitTransactionAsync(_context);
                    tryWithDeletes = false;
                }
                catch(RequestFailedException ex) when (ex.Status == 404)
                {
                }

                // In that case, we want to try and add the deleted items,
                if(tryWithDeletes)
                {
                    await _table.SubmitTransactionAsync(_deleteBackupContext);
                    await _table.SubmitTransactionAsync(_context);
                }
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "EntityAlreadyExists" || ex.Message.Contains("The specified entity already exists"))
            {
                throw new StorageEntityAlreadyExistsException(ex.ErrorCode, ex);
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
            data = Array.Empty<byte>();
        }
        else
        {
            data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(entity.DataObject));

            if (_encryptor != null)
            {
                using var ms = new MemoryStream();
                using (var enc = _encryptor.Encrypt(ms, false))
                {
                    enc.Write(data, 0, data.Length);
                }

                data = ms.ToArray();
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