using Kalix.Leo.Storage;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    /// <summary>
    /// A partition that is build around a type, this type is serialised using Json serialisation
    /// and is assumed to have an integer id that defines uniqueness
    /// </summary>
    /// <typeparam name="T">The type of the object, must be configured in the leo engine configuration</typeparam>
    public interface IObjectPartition<T> : IBasePartition
        where T : ObjectWithAuditInfo
    {
        /// <summary>
        /// Save a record at the specified id value, metadata is completely overriden
        /// </summary>
        /// <param name="data">The object to save</param>
        /// <param name="id">The id to save the record as</param>
        /// <param name="audit">Audit information to save against this record</param>
        /// <param name="metadata">Any additional metadata to save (Note this is NOT encrypted)</param>
        /// <returns>Task that completes when the record is saved</returns>
        Task<ObjectPartitionWriteResult<T>> Save(T data, long id, UpdateAuditInfo audit, Metadata metadata = null);

        /// <summary>
        /// Update metadata at the specified id value, does not override it
        /// </summary>
        /// <param name="id">The id location to save the record (in this particular partition)</param>
        /// <param name="metadata">metadata to save - note this is NOT encrypted</param>
        /// <returns>Task that completes when the metadata is saved</returns>
        Task<Metadata> SaveMetadata(long id, Metadata metadata);

        /// <summary>
        /// Save a new or existing record record
        /// </summary>
        /// <param name="data">The data to save</param>
        /// <param name="idField">The expression to access the id field on the object</param>
        /// <param name="audit">Audit information to save against this record</param>
        /// <param name="preSaveProcessing">An optional step to do some processing after the id has been created but before the record is saved</param>
        /// <param name="metadata">Any additional metadata to save (Note this is NOT encrypted)</param>
        /// <returns>Task that completes when the record is saved, returns the id</returns>
        Task<ObjectPartitionWriteResult<T>> Save(T data, Expression<Func<T, long?>> idField, UpdateAuditInfo audit, Action<long> preSaveProcessing = null, Metadata metadata = null);

        /// <summary>
        /// Save a new or existing record record
        /// </summary>
        /// <param name="data">The data to save</param>
        /// <param name="idField">The expression to access the id field on the object</param>
        /// <param name="audit">Audit information to save against this record</param>
        /// <param name="preSaveProcessing">An optional step to do some processing after the id has been created but before the record is saved</param>
        /// <param name="metadata">Any additional metadata to save (Note this is NOT encrypted)</param>
        /// <returns>Task that completes when the record is saved, returns the id</returns>
        Task<ObjectPartitionWriteResult<T>> Save(T data, Expression<Func<T, long?>> idField, UpdateAuditInfo audit, Func<long, Task> preSaveProcessing = null, Metadata metadata = null);

        /// <summary>
        /// Load a record by id
        /// </summary>
        /// <param name="id">The id of the record</param>
        /// <param name="snapshot">Specific snapshot to load</param>
        /// <returns>The object along with any metadata (or null)</returns>
        Task<ObjectWithMetadata<T>> Load(long id, string snapshot = null);

        /// <summary>
        /// Just get the metadata associated with an object
        /// </summary>
        /// <param name="id">The id of the record</param>
        /// <param name="snapshot">Specific snapshot to load</param>
        /// <returns>The metadata or null</returns>
        Task<Metadata> GetMetadata(long id, string snapshot = null);

        /// <summary>
        /// Find any snapshots associated with a specific id
        /// </summary>
        /// <param name="id">The id to search for snapshots in</param>
        /// <returns>A list of snapshot ids and metadata</returns>
        IAsyncEnumerable<Snapshot> FindSnapshots(long id);

        /// <summary>
        /// Find all the records in a specified partition
        /// </summary>
        /// <returns>The ids and metadata for each record in this partition</returns>
        IAsyncEnumerable<IdWithMetadata> FindAll();

        /// <summary>
        /// Delete a record with the specified id (will not delete snapshots though)
        /// </summary>
        /// <param name="id">The id of the record to delete</param>
        /// <param name="audit">Audit information of the person deleting</param>
        /// <returns>Task that completes when the delete is successful</returns>
        Task Delete(long id, UpdateAuditInfo audit);

        /// <summary>
        /// Delete a record with the specified id, and any snapshots as well
        /// </summary>
        /// <param name="id">The id of the record to delete</param>
        /// <returns>Task that completes when ALL deletes are successful</returns>
        Task DeletePermanent(long id);

        /// <summary>
        /// Will send out an index message for this item, even if there was no data saved
        /// </summary>
        /// <param name="id">Id of the object</param>
        /// <param name="metadata">Optional metadata, required metadata will be added automatically</param>
        /// <returns>Task when queue message has been sent</returns>
        Task ForceIndex(long id, Metadata metadata = null);

        /// <summary>
        /// If this partition is configured to index, will find all records and queue an index message
        /// </summary>
        /// <returns>Task that completes when all records have been queued to index</returns>
        Task ReIndexAll();

        /// <summary>
        /// If this partition is configured to backup, will find all records and queue a backup message
        /// </summary>
        /// <returns>Task that completes when all records have been queued to backup</returns>
        Task ReBackupAll();

        /// <summary>
        /// Advanced - Set the internal id generator to the specified value
        /// THIS CAN CAUSE HAVOC IF DONE INCORRECTLY (ie records will start to override each other)
        /// </summary>
        /// <param name="newId">Id to set the generator to</param>
        /// <returns>Task when the generator has been changed</returns>
        Task SetInternalIdGenerator(long newId);
    }

    /// <summary>
    /// Save result so that you can get the Id and metadata
    /// </summary>
    /// <typeparam name="T">The type of the object</typeparam>
    public class ObjectPartitionWriteResult<T>
        where T : ObjectWithAuditInfo
    {
        /// <summary>
        /// Id of the newly saved object
        /// </summary>
        public long Id { get; set; }

        /// <summary>
        /// Data and metadata gained
        /// </summary>
        public ObjectWithMetadata<T> Data { get; set; }
    }
}
