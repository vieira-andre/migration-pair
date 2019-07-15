using Cassandra;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Text;

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
            GetColumnsForTable(ref ctable);
            GetRows(ref ctable);

            var tableData = WriteResultsToObject(ctable);
            SaveResultsIntoFile(tableData, filePath);

            session.Dispose();
            cluster.Dispose();
        }

        private static void GetColumnsForTable(ref CTable ctable)
        {
            string cql = "SELECT * from system.schema_columns WHERE columnfamily_name=? ALLOW FILTERING";
            PreparedStatement pStatement = session.Prepare(cql);

            BoundStatement bStatement = pStatement.Bind(ctable.Name);
            RowSet results = session.Execute(bStatement);

            foreach (Row result in results)
            {
                var columnName = result.GetValue<string>("column_name");
                var columnType = GetColumnDataType(ctable, columnName);

                ctable.Columns.Add(new CColumn(columnName, columnType));
            }
        }

        private static Type GetColumnDataType(CTable ctable, string columnName)
        {
            string cql = $"SELECT {columnName} from {ctable.Keyspace}.{ctable.Name}";
            var statement = new SimpleStatement(cql);

            RowSet results = session.Execute(statement);
            return results.Columns[0].Type;
        }

        private static void GetRows(ref CTable ctable)
        {
            string cql = $"select * from {ctable.Keyspace}.{ctable.Name}";
            var statement = new SimpleStatement(cql);
            RowSet results = session.Execute(statement);

            foreach (Row result in results)
            {
                CField[] row = new CField[result.Length];

                for (int i = 0; i < result.Length; i++)
                    row[i] = new CField(result[i], results.Columns[i].Type);

                ctable.Rows.Add(row);
            }
        }

        private static StringBuilder WriteResultsToObject(CTable ctable)
        {
            var tableData = new StringBuilder();

            foreach (CField[] row in ctable.Rows)
            {
                var rowToWrite = new List<string>();

                foreach (CField cfield in row)
                {
                    string valueToWrite = Convert.ToString(cfield.Value);

                    if (cfield.DataType.Equals(typeof(string)) && !string.IsNullOrEmpty(valueToWrite))
                        valueToWrite = string.Format("\"{0}\"", valueToWrite.Replace("\"", "\"\""));

                    rowToWrite.Add(valueToWrite);
                }

                tableData.AppendLine(string.Join(",", rowToWrite));
            }

            return tableData;
        }

        private static void SaveResultsIntoFile(StringBuilder tableData, string filePath)
        {
            File.WriteAllText(filePath, tableData.ToString());
        }
    }

    internal class CTable
    {
        public string Name { get; set; }
        public string Keyspace { get; set; }
        public List<CColumn> Columns { get; set; }
        public List<CField[]> Rows { get; set; }

        public CTable(string name, string keyspace)
        {
            Name = name;
            Keyspace = keyspace;
            Columns = new List<CColumn>();
            Rows = new List<CField[]>();
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

    internal class CField
    {
        public dynamic Value { get; set; }
        public Type DataType { get; set; }

        public CField(dynamic value, Type dataType)
        {
            Value = value;
            DataType = dataType;
        }
    }
}
