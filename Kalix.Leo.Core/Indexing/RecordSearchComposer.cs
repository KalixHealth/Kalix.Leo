using Kalix.Leo.Indexing.Config;
using Kalix.Leo.Table;

namespace Kalix.Leo.Indexing
{
    public class RecordSearchComposer : IRecordSearchComposer
    {
        private readonly ITableClient _client;

        public RecordSearchComposer(ITableClient client)
        {
            _client = client;
        }

        public IRecordSearch ComposeSearch(string prefix)
        {
            return new RecordSearch(prefix);
        }

        public IRecordUniqueIndex<T> ComposeUniqueIndex<T>(string prefix)
        {
            return new RecordUniqueIndex<T>(prefix);
        }

        public IRecordSearchConfig<TMain> Compose<TMain>(string tableName)
        {
            return new RecordSearchConfig<TMain>(_client, tableName);
        }
    }
}