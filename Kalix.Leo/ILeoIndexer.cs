using Kalix.Leo.Lucene;
using System.Threading.Tasks;

namespace Kalix.Leo
{
    public interface ILeoIndexer
    {
        Task Index(StoreDataDetails details, ILuceneIndex defaultIndex);
    }
}
