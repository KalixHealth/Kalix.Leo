using System.Threading.Tasks;

namespace Kalix.Leo.Storage
{
    public interface IUniqueIdGenerator
    {
        Task<long> NextId();
    }
}
