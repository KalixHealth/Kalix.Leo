﻿using Kalix.Leo.Encryption;
using Kalix.Leo.Indexing;
using System.Threading.Tasks;

namespace Kalix.Leo;

/// <summary>
/// The Leo engine, a convention based system that partitions all of it's data and provides a pipeline for
/// encryption, compression, indexing and backups.
/// </summary>
public interface ILeoEngine
{
    /// <summary>
    /// Use the composer to build up index plans based on table storage (use it with the partition.Index method)
    /// </summary>
    IRecordSearchComposer Composer { get; }

    /// <summary>
    /// Open up a document partition for a specified base path 
    /// 
    /// Note that this object is internally cached by the engine for performance.
    /// </summary>
    /// <param name="basePath">Identifies this partition from the items configuration of the engine</param>
    /// <param name="partitionId">The id to seperate all records saved to this partition</param>
    /// <returns>Document partition that can be used to save/load/search etc</returns>
    IDocumentPartition GetDocumentPartition(string basePath, long partitionId);

    /// <summary>
    /// Open up an object partition for a specified type
    /// 
    /// Note that this object is internally cached by the engine for performance.
    /// </summary>
    /// <typeparam name="T">The type that identifies the item configuration to use</typeparam>
    /// <param name="partitionId">The id to seperate all records saved to this partition</param>
    /// <returns>Object partition that can be used to save/load/search etc</returns>
    IObjectPartition<T> GetObjectPartition<T>(long partitionId) where T : ObjectWithAuditInfo;

    /// <summary>
    /// Retrieve the encryptor used for a given partition, can be useful if you want to use tables/store manually
    /// </summary>
    /// <param name="partitionId">The id to seperate all records saved to this partition</param>
    /// <returns>Encryptor for the given partition</returns>
    Task<IEncryptor> GetEncryptor(long partitionId);
}