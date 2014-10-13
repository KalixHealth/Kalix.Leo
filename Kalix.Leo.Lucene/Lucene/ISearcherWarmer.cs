using Lucene.Net.Search;

//https://github.com/NielsKuhnel/NrtManager
namespace Lucene.Net.Contrib.Management
{
    public interface ISearcherWarmer
    {
        void Warm(IndexSearcher s);
    }
}
