using Kalix.Leo.Indexing.Config;

namespace Kalix.Leo.Indexing
{
    public interface IRecordSearchComposer
    {
        IRecordSearch ComposeSearch(string prefix);
        IRecordUniqueIndex<T> ComposeUniqueIndex<T>(string prefix);
        IRecordSearchConfig<TMain> Compose<TMain>(string tableName);
    }
}