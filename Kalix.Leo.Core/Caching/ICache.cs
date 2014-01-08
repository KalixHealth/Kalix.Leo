using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Caching
{
    public interface ICache
    {
        Task<T> Get<T>(string key, Func<Task<T>> fallback = null);
        Task Clear(string key);
    }
}
