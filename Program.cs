using Cassandra;
using CsvHelper;
using migration_pair.Helpers;
using migration_pair.Models;
using migration_pair.Policies;
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
        private static readonly Log logger = new Log(config.LogFilePath);

        #region Clusters & sessions
        private static ICluster sourceCluster, targetCluster;
        private static ISession sourceSession, targetSession;
        #endregion

        static void Main(string[] args)
        {
            Enum.TryParse(config.TaskToPerform.Value, true, out TaskToPerform procedure);
            logger.Write(procedure, config);

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
                    logger.Write($"[Error] Config entry {config.TaskToPerform.Path} is either unspecified or misspecified.");
                    break;
            }

            logger.Write("Ending application...");
        }

        private static void BuildSourceClusterAndSession()
        {
            if (sourceSession == null)
            {
                logger.Write("Building source cluster and connecting session...");

                sourceCluster = Cluster.Builder().WithPort(config.SourcePort).AddContactPoints(config.SourceEndPoints).Build();
                sourceSession = sourceCluster.Connect();
            }
        }

        private static void BuildTargetClusterAndSession()
        {
            if (targetSession == null)
            {
                logger.Write("Building target cluster and connecting session...");

                targetCluster = Cluster.Builder()
                                .WithPort(config.TargetPort)
                                .WithRetryPolicy(new RetryPolicy())
                                .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0))
                                .AddContactPoints(config.TargetEndPoints)
                                .Build();

                targetSession = targetCluster.Connect();
            }
        }

        private static void ExtractionPhase()
        {
            logger.Write("Starting extraction phase...");

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
                    logger.Write($"[Exception] {ex.ToString()}");
            }
            catch (Exception ex)
            {
                logger.Write($"[Exception] {ex.ToString()}");
            }
            finally
            {
                DisposeSourceSessionAndCluster();
            }
        }

        private static void InsertionPhase()
        {
            logger.Write("Starting insertion phase...");

            try
            {
                IList<string[]> tableData = ReadFromFile(config.FilePath);

                BuildTargetClusterAndSession();

                IList<CColumn> columns = GetColumnsInfo(config.TargetKeyspace, config.TargetTable);
                InsertDataIntoTable(ref tableData, ref columns);
            }
            catch (AggregateException aggEx)
            {
                foreach (Exception ex in aggEx.Flatten().InnerExceptions)
                    logger.Write($"[Exception] {ex.ToString()}");
            }
            catch (Exception ex)
            {
                logger.Write($"[Exception] {ex.ToString()}");
            }
            finally
            {
                DisposeTargetSessionAndCluster();
            }
        }

        private static void GetRows(ref CTable ctable)
        {
            logger.Write("Getting rows from source table...");

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

            logger.Write($"Rows retrieved: {ctable.Rows.Count}");
        }

        private static StringBuilder WriteResultsToObject(CTable ctable)
        {
            logger.Write("Writing extraction results to object...");

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
            logger.Write("Saving extraction results into file...");

            _ = Directory.CreateDirectory(Path.GetDirectoryName(filePath));
            File.WriteAllText(filePath, tableData.ToString());
        }

        private static IList<string[]> ReadFromFile(string filePath)
        {
            logger.Write("Reading data from file...");

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

            logger.Write($"Rows retrieved: {tableData.Count}");

            return tableData;
        }

        private static IList<CColumn> GetColumnsInfo(string keyspace, string table)
        {
            logger.Write($"Getting columns info: [table] {table} [keyspace] {keyspace}");

            var columns = new List<CColumn>();

            string cql = $"SELECT * FROM {keyspace}.{table} LIMIT 1";
            var statement = new SimpleStatement(cql);
            RowSet results = targetSession.Execute(statement);

            foreach (CqlColumn column in results.Columns)
                columns.Add(new CColumn(column.Name, column.Type));

            return columns;
        }

        private static void InsertDataIntoTable(ref IList<string[]> tableData, ref IList<CColumn> columns)
        {
            logger.Write("Inserting data into target table...");

            string columnsAsString = string.Join(',', columns.GroupBy(c => c.Name).Select(c => c.Key));
            string valuesPlaceholders = string.Concat(Enumerable.Repeat("?,", columns.Count)).TrimEnd(',');

            string cql = $"INSERT INTO {config.TargetKeyspace}.{config.TargetTable} ({columnsAsString}) VALUES ({valuesPlaceholders})";
            PreparedStatement pStatement = targetSession.Prepare(cql);

            var insertStatements = new List<BoundStatement>();

            foreach (string[] row in tableData)
            {
                dynamic[] preparedRow = new dynamic[row.Length];

                int i = 0;
                while (i < row.Length)
                {
                    preparedRow[i] = DynamicTypeConverter.Convert(row[i], columns[i].DataType);
                    i++;
                }

                BoundStatement bStatement = pStatement.Bind(preparedRow);
                insertStatements.Add(bStatement);
            }

            ExecuteInsertAsync(insertStatements).Wait();
        }

        private static async Task ExecuteInsertAsync(IList<BoundStatement> insertStatements)
        {
            var tasks = new ConcurrentQueue<Task>();

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            foreach (var stmt in insertStatements)
            {
                if (IsRequestsLimitReached())
                {
                    while (CurrentInFlightQueries() > MaxRequestsPerConnection() / 2)
                        await Task.Delay(10);
                }

                tasks.Enqueue(targetSession.ExecuteAsync(stmt));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            stopwatch.Stop();
            logger.Write($"Elapsed insertion time: {stopwatch.ElapsedMilliseconds} ms");
        }

        private static bool IsRequestsLimitReached()
        {
            return CurrentInFlightQueries() >= targetSession.Cluster.Configuration.PoolingOptions.GetMaxRequestsPerConnection()
                   ? true
                   : false;
        }

        private static int CurrentInFlightQueries()
        {
            ISessionState state = targetSession.GetState();
            int currentInFlightQueries = 0;

            foreach (var host in state.GetConnectedHosts())
                currentInFlightQueries += state.GetInFlightQueries(host);

            return currentInFlightQueries;
        }

        private static int MaxRequestsPerConnection() => targetSession.Cluster.Configuration.PoolingOptions.GetMaxRequestsPerConnection();

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
                logger.Write("[Error] Tables from source and target have divergent number of columns.");
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
                logger.Write("Tables are compliant with each other.");
                return true;
            }
            else
                logger.Write($"Tables are not compliant with each other: {sourceColumns.Count - matches.Count} mismatch(es) among {sourceColumns.Count} columns.");

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