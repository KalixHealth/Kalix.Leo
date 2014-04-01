using System;

namespace Kalix.Leo.Listeners
{
    /// <summary>
    /// Listener to listen for index messages and act on them
    /// </summary>
    public interface IIndexListener
    {
        /// <summary>
        /// Register an indexer for records based on a certain base path
        /// </summary>
        /// <param name="basePath">The base path that this indexer should listen for</param>
        /// <param name="indexer">The type of the indexer to use</param>
        void RegisterPathIndexer(string basePath, Type indexer);
        
        /// <summary>
        /// Register an indexer based on a record specified by type
        /// </summary>
        /// <typeparam name="T">The type to listen to for this indexer</typeparam>
        /// <param name="indexer">The indexer to use for this type</param>
        void RegisterTypeIndexer<T>(Type indexer);

        /// <summary>
        /// Register an indexer based on a record specified by type
        /// </summary>
        /// <param name="type">The type to listen to for this indexer</param>
        /// <param name="indexer">The indexer to use for this type</param>
        void RegisterTypeIndexer(Type type, Type indexer);

        /// <summary>
        /// Start a listener on an index queue and handle the messages
        /// </summary>
        /// <param name="uncaughtException">Use this to listen to failed messages</param>
        /// <param name="messagesToProcessInParallel">Number of messages to handle concurrently</param>
        /// <returns>A disposable that will stop the listener if disposed</returns>
        IDisposable StartListener(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null);
    }
}
