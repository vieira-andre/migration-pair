namespace Mycenae.Models
{
    public class SettingsModel
    {
        private int insertionBatch;

        public TaskToPerform TaskToPerform { get; set; }
        public Connections Connections { get; set; }
        public DataFiles Files { get; set; }
        public int InsertionBatch { get => insertionBatch; set => insertionBatch = (value > 0) ? value : 100000; }
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
        private int port;

        public string[] Endpoints { get; set; }
        public int Port { get => port; set { port = (value > 0) ? value : Cassandra.ProtocolOptions.DefaultPort; } }
        public string Keyspace { get; set; }
        public string Table { get; set; }
    }

    public class DataFile
    {
        public string Path { get; set; }
        public bool HasHeader { get; set; }
    }
}
