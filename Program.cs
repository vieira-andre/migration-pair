using Cassandra;
using CsvHelper;
using migration_pair.Helpers;
using migration_pair.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace migration_pair
{
    class Program
    {
        private static readonly ConfigurableValues config = new ConfigurableValues();

        #region Clusters & sessions
        private static ICluster sourceCluster, targetCluster;
        private static ISession sourceSession, targetSession;
        #endregion

        static void Main(string[] args)
        {
            Enum.TryParse(config.TaskToPerform, true, out TaskToPerform procedure);
            Log.Write(procedure, config);

            switch (procedure)
            {
                case TaskToPerform.Extract:
                    ExtractionPhase();
                    break;

                case TaskToPerform.Insert:
                    InsertionPhase();
                    break;

                case TaskToPerform.ExtractAndInsert:
                    ExtractAndInsert();
                    break;

                default:
                    Log.Write("[Error] Config entry \"Task_To_Perform\" is either unspecified or misspecified.");
                    break;
            }

            Log.Write("Ending application...");
        }

        private static void BuildSourceClusterAndSession()
        {
            if (sourceSession == null)
            {
                Log.Write("Building source cluster and connecting session...");

                sourceCluster = Cluster.Builder().WithPort(config.SourcePort).AddContactPoints(config.SourceEndPoints).Build();
                sourceSession = sourceCluster.Connect();
            }
        }

        private static void BuildTargetClusterAndSession()
        {
            if (targetSession == null)
            {
                Log.Write("Building target cluster and connecting session...");

                targetCluster = Cluster.Builder().WithPort(config.TargetPort).AddContactPoints(config.TargetEndPoints).Build();
                targetSession = targetCluster.Connect();
            }
        }

        private static void ExtractionPhase()
        {
            Log.Write("Starting extraction phase...");

            try
            {
                BuildSourceClusterAndSession();

                var ctable = new CTable(config.SourceTable, config.SourceKeyspace);
                GetRows(ref ctable);

                var tableData = WriteResultsToObject(ctable);
                SaveResultsIntoFile(ref tableData, config.FilePath);
            }
            catch (AggregateException aggEx)
            {
                foreach (Exception ex in aggEx.Flatten().InnerExceptions)
                    Log.Write($"[Exception] {ex.ToString()}");
            }
            catch (Exception ex)
            {
                Log.Write($"[Exception] {ex.ToString()}");
            }
            finally
            {
                DisposeSourceSessionAndCluster();
            }
        }

        private static void InsertionPhase()
        {
            Log.Write("Starting insertion phase...");

            try
            {
                IList<string[]> tableData = ReadFromFile(config.FilePath);

                BuildTargetClusterAndSession();

                IList<CColumn> columns = GetColumnsInfo(config.TargetKeyspace, config.TargetTable);
                InsertDataIntoTableAsync(tableData, columns).Wait();
            }
            catch (AggregateException aggEx)
            {
                foreach (Exception ex in aggEx.Flatten().InnerExceptions)
                    Log.Write($"[Exception] {ex.ToString()}");
            }
            catch (Exception ex)
            {
                Log.Write($"[Exception] {ex.ToString()}");
            }
            finally
            {
                DisposeTargetSessionAndCluster();
            }
        }

        private static void GetRows(ref CTable ctable)
        {
            Log.Write("Getting rows from source table...");

            string cql = $"SELECT * FROM {ctable.Keyspace}.{ctable.Name}";
            var statement = new SimpleStatement(cql);
            RowSet results = sourceSession.Execute(statement);

            foreach (Row result in results)
            {
                CField[] row = new CField[result.Length];

                for (int i = 0; i < result.Length; i++)
                {
                    row[i] = results.Columns[i].Type.IsAssignableFrom(typeof(DateTimeOffset))
                        ? new CField(((DateTimeOffset)result[i]).ToUnixTimeMilliseconds(), results.Columns[i].Name, typeof(long))
                        : new CField(result[i], results.Columns[i].Name, results.Columns[i].Type);
                }

                ctable.Rows.Add(row);
            }

            Log.Write($"Rows retrieved: {ctable.Rows.Count}");
        }

        private static StringBuilder WriteResultsToObject(CTable ctable)
        {
            Log.Write("Writing extraction results to object...");

            var tableData = new StringBuilder();

            foreach (CField[] row in ctable.Rows)
            {
                var rowToWrite = new List<string>(row.Length);

                foreach (CField cfield in row)
                {
                    string valueToWrite = Convert.ToString(cfield.Value);

                    if (cfield.DataType.Equals(typeof(string)) && !string.IsNullOrEmpty(valueToWrite))
                        valueToWrite = string.Format("\"{0}\"", valueToWrite.Replace("\"", "\"\""));

                    rowToWrite.Add(valueToWrite);
                }

                tableData.AppendLine(string.Join(',', rowToWrite));
            }

            return tableData;
        }

        private static void SaveResultsIntoFile(ref StringBuilder tableData, string filePath)
        {
            Log.Write("Saving extraction results into file...");

            _ = Directory.CreateDirectory(Path.GetDirectoryName(filePath));
            File.WriteAllText(filePath, tableData.ToString());
        }

        private static IList<string[]> ReadFromFile(string filePath)
        {
            Log.Write("Reading data from file...");

            if (!File.Exists(filePath))
                throw new FileNotFoundException("The file either does not exist or there is a lack of permissions to read it. Check the path provided.");

            var tableData = new List<string[]>();

            using (TextReader reader = new StreamReader(filePath))
            using (var csvReader = new CsvReader(reader))
            {
                csvReader.Configuration.Delimiter = ",";
                csvReader.Configuration.HasHeaderRecord = false;
                csvReader.Configuration.MissingFieldFound = null;

                var records = csvReader.GetRecords<dynamic>();

                foreach (IDictionary<string, object> record in records)
                {
                    var row = new List<string>(record.Values.Count);

                    foreach (string value in record.Values)
                        row.Add(value);

                    tableData.Add(row.ToArray());
                }
            }

            Log.Write($"Rows retrieved: {tableData.Count}");

            return tableData;
        }

        private static IList<CColumn> GetColumnsInfo(string keyspace, string table)
        {
            Log.Write($"Getting columns info: [table] {table} [keyspace] {keyspace}");

            var columns = new List<CColumn>();

            string cql = $"SELECT * FROM {keyspace}.{table} LIMIT 1";
            var statement = new SimpleStatement(cql);
            RowSet results = targetSession.Execute(statement);

            foreach (CqlColumn column in results.Columns)
                columns.Add(new CColumn(column.Name, column.Type));

            return columns;
        }

        private static async Task InsertDataIntoTableAsync(IList<string[]> tableData, IList<CColumn> columns)
        {
            Log.Write("Inserting data into target table...");

            string columnsAsString = string.Join(',', columns.GroupBy(c => c.Name).Select(c => c.Key));
            string valuesPlaceholders = string.Concat(Enumerable.Repeat("?,", columns.Count)).TrimEnd(',');

            string cql = $"INSERT INTO {config.TargetKeyspace}.{config.TargetTable} ({columnsAsString}) VALUES ({valuesPlaceholders})";
            PreparedStatement pStatement = targetSession.Prepare(cql);

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var tasks = new ConcurrentQueue<Task>();

            foreach (string[] row in tableData)
            {
                dynamic[] preparedRow = new dynamic[row.Length];

                int i = 0;
                while (i < row.Length)
                {
                    preparedRow[i] = DynamicTypeConverter.Convert(row[i], columns[i].DataType);
                    i++;
                }

                if (IsRequestsLimitReached()) await Task.Delay(200);

                BoundStatement bStatement = pStatement.Bind(preparedRow);
                tasks.Enqueue(targetSession.ExecuteAsync(bStatement));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            stopwatch.Stop();
            Log.Write($"Elapsed insertion time: {stopwatch.ElapsedMilliseconds} ms");
        }

        private static bool IsRequestsLimitReached()
        {
            ISessionState state = targetSession.GetState();

            foreach (var host in state.GetConnectedHosts())
            {
                if (state.GetInFlightQueries(host) >= targetSession.Cluster.Configuration.PoolingOptions.GetMaxRequestsPerConnection())
                    return true;
            }

            return false;
        }

        private static void ExtractAndInsert()
        {
            BuildSourceClusterAndSession();
            BuildTargetClusterAndSession();

            if (IsThereCompliance())
            {
                ExtractionPhase();
                InsertionPhase();
            }

            DisposeSourceSessionAndCluster();
            DisposeTargetSessionAndCluster();
        }

        private static bool IsThereCompliance()
        {
            IList<CColumn> sourceColumns = GetColumnsInfo(config.SourceKeyspace, config.SourceTable);
            IList<CColumn> targetColumns = GetColumnsInfo(config.TargetKeyspace, config.TargetTable);

            if (sourceColumns.Count != targetColumns.Count)
            {
                Log.Write("[Error] Tables from source and target have divergent number of columns.");
                return false;
            }

            var matches = new List<bool>();

            foreach (CColumn sourceColumn in sourceColumns)
            {
                foreach (CColumn targetColumn in targetColumns)
                {
                    if (sourceColumn.Name.Equals(targetColumn.Name)
                        && sourceColumn.DataType.Equals(targetColumn.DataType))
                    {
                        matches.Add(true);
                    }
                }
            }

            if (matches.Count == sourceColumns.Count)
            {
                Log.Write("Tables are compliant with each other.");
                return true;
            }
            else
                Log.Write($"Tables are not compliant with each other: {sourceColumns.Count - matches.Count} mismatch(es) among {sourceColumns.Count} columns.");

            return false;
        }

        private static void DisposeSourceSessionAndCluster()
        {
            if (sourceSession != null && !sourceSession.IsDisposed)
            {
                sourceSession.Dispose();
                sourceCluster.Dispose();
            }
        }

        private static void DisposeTargetSessionAndCluster()
        {
            if (targetSession != null && !targetSession.IsDisposed)
            {
                targetSession.Dispose();
                targetCluster.Dispose();
            }
        }
    }
}