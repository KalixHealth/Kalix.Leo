using System.Threading.Tasks;

namespace Kalix.Leo.Indexing
{
    public interface IIndexer
    {
        Task Index(StoreDataDetails details);
    }

    public interface IIndexer<T> : IIndexer { }
}
