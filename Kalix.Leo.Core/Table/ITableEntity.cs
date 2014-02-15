namespace Kalix.Leo.Table
{
    public interface ITableEntity
    {
        string PartitionKey { get; }
        string RowKey { get; }

        object DataObject { get; }
    }

    public interface ITableEntity<T> : ITableEntity
    {
        T Data { get; }
    }

    public sealed class TableEntity<T> : ITableEntity<T>
    {
        private readonly string _partitionKey;
        private readonly string _rowKey;
        private readonly T _data;

        public TableEntity(string partitionKey, string rowKey, T data)
        {
            _partitionKey = partitionKey;
            _rowKey = rowKey;
            _data = data;
        }

        public string PartitionKey
        {
            get { return _partitionKey; }
        }

        public string RowKey
        {
            get { return _rowKey; }
        }

        public T Data
        {
            get { return _data; }
        }

        public object DataObject
        {
            get { return _data; }
        }
    }
}
