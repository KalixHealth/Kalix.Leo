using System.Threading.Tasks;

namespace Kalix.Leo.Storage
{
    public interface IUniqueIdGenerator
    {
        Task<long> NextId();

        Task SetCurrentId(long newId);
    }
}
