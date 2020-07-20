namespace Mycenae.Models
{
    public class SettingsModel
    {
        private int _insertionBatch;

        public TaskToExecute TaskToExecute { get; set; }
        public Connections Connections { get; set; }
        public DataFiles Files { get; set; }
        public int InsertionBatch { get => _insertionBatch; set => _insertionBatch = value > 0 ? value : 100000; }
    }

    public class Connections
    {
        public Connection Source { get; set; }
        public Connection Target { get; set; }
    }

    public class DataFiles
    {
        public DataFile Extraction { get; set; }
        public DataFile Insertion { get; set; }
    }

    public class Connection
    {
        private int _port;

        public string[] Endpoints { get; set; }
        public int Port { get => _port; set => _port = value > 0 ? value : Cassandra.ProtocolOptions.DefaultPort; }
        public string Keyspace { get; set; }
        public string Table { get; set; }
    }

    public class DataFile
    {
        private string _delimiter;

        public string Path { get; set; }
        public bool HasHeader { get; set; }
        public string Delimiter { get => _delimiter; set => _delimiter = string.IsNullOrEmpty(value) ? "," : value; }
    }
}
