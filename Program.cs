using Cassandra;

namespace migration_pair
{
    class Program
    {
        private static readonly string[] endpoints = { "localhost" };
        private static readonly string keyspace = null, tableName = null;
        private static readonly string filePath = null;

        private static Cluster cluster = Cluster.Builder().AddContactPoints(endpoints).Build();
        private static readonly ISession session = cluster.Connect();

        static void Main(string[] args)
        {

        }
    }
}
