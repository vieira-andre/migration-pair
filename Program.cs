using Cassandra;
using System;
using System.Collections.Generic;

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

    internal class CTable
    {
        public string Name { get; set; }
        public string Keyspace { get; set; }
        public List<CColumn> Columns { get; set; }

        public CTable(string name, string keyspace)
        {
            Name = name;
            Keyspace = keyspace;
            Columns = new List<CColumn>();
        }
    }

    internal class CColumn
    {
        public string Name { get; set; }
        public Type Type { get; set; }

        public CColumn(string name, Type type)
        {
            Name = name;
            Type = type;
        }
    }
}
