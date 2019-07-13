using Cassandra;
using System;
using System.Collections.Generic;
using System.Configuration;

namespace migration_pair
{
    class Program
    {
        private static readonly string[] endpoints = ConfigurationManager.AppSettings["Endpoints"].Split(',');
        private static readonly string keyspace = ConfigurationManager.AppSettings["Keyspace"];
        private static readonly string tableName = ConfigurationManager.AppSettings["Table_Name"];
        private static readonly string filePath = ConfigurationManager.AppSettings["File_Path"];

        private static Cluster cluster = Cluster.Builder().AddContactPoints(endpoints).Build();
        private static readonly ISession session = cluster.Connect();

        static void Main(string[] args)
        {
            var ctable = new CTable(tableName, keyspace);
            ctable = GetColumnsForTable(ctable);
			
            session.Dispose();
            cluster.Dispose();
        }

        static CTable GetColumnsForTable(CTable ctable)
        {
            string cql = ConfigurationManager.AppSettings["Select_ColumnFamily"];
            PreparedStatement pStatement = session.Prepare(cql);

            BoundStatement bStatement = pStatement.Bind(ctable.Name);
            RowSet results = session.Execute(bStatement);

            return ctable;
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
