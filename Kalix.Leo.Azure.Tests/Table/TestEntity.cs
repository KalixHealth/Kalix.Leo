using Kalix.Leo.Table;

namespace Kalix.Leo.Azure.Tests.Table
{
    public class TestEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public object DataObject { get; set; }
    }
}
