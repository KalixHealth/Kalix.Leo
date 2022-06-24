using System.Threading.Tasks;

namespace Kalix.Leo.Storage;

/// <summary>
/// An interface that is capable of generating unique sequential ids
/// This is generally based on a store so these ids are unique accross instances
/// </summary>
public interface IUniqueIdGenerator
{
    /// <summary>
    /// Gets the next id
    /// </summary>
    /// <returns>The next id</returns>
    Task<long> NextId();

    /// <summary>
    /// Advanced - override the current generation id
    /// </summary>
    /// <param name="newId">The id to set the generator to</param>
    /// <returns>A Task that is complete when the new id has been set</returns>
    Task SetCurrentId(long newId);
}