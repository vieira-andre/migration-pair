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
            ctable = GetValuesForColumns(ctable);

            session.Dispose();
            cluster.Dispose();
        }

        static CTable GetColumnsForTable(CTable ctable)
        {
            string cql = "SELECT * from system.schema_columns WHERE columnfamily_name=? ALLOW FILTERING";
            PreparedStatement pStatement = session.Prepare(cql);

            BoundStatement bStatement = pStatement.Bind(ctable.Name);
            RowSet results = session.Execute(bStatement);

            foreach (Row result in results.GetRows())
            {
                var columnName = result.GetValue<string>("column_name");
                var columnType = GetColumnDataType(ctable, columnName);

                ctable.Columns.Add(new CColumn(columnName, columnType));
            }

            return ctable;
        }

        static Type GetColumnDataType(CTable ctable, string columnName)
        {
            string cql = $"SELECT {columnName} from {ctable.Keyspace}.{ctable.Name}";
            var statement = new SimpleStatement(cql);

            RowSet results = session.Execute(statement);
            return results.Columns[0].Type;
        }

        static CTable GetValuesForColumns(CTable ctable)
        {
            string cql = $"select * from {ctable.Keyspace}.{ctable.Name}";
            var statement = new SimpleStatement(cql);
            RowSet results = session.Execute(statement);

            foreach (Row result in results.GetRows())
            {
                foreach (CColumn column in ctable.Columns)
                {
                    column.Values.Add(result.GetValue<dynamic>(column.Name));
                }
            }

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
