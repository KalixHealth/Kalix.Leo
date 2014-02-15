using Kalix.Leo.Encryption;
using System.Threading.Tasks;

namespace Kalix.Leo.Table
{
    public interface ITableContext
    {
        void Replace(ITableEntity entity);
        void Delete(ITableEntity entity);
        void Insert(ITableEntity entity);
        void InsertOrMerge(ITableEntity entity);
        void InsertOrReplace(ITableEntity entity);
        Task Save();
    }
}
