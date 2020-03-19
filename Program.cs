using Cassandra;
using CsvHelper;
using migration_pair.Helpers;
using migration_pair.Models;
using migration_pair.Policies;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Logger = NLog.Logger;

namespace migration_pair
{
    class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        #region Clusters & sessions
        private static ICluster _sourceCluster, _targetCluster;
        private static ISession _sourceSession, _targetSession;
        #endregion

        static void Main()
        {
            if (!Enum.TryParse(Config.TaskToPerform.Value, true, out TaskToPerform procedure))
                Logger.Error($"Config entry {Config.TaskToPerform.Path} is either unspecified or misspecified.");

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
                    break;
            }

            Logger.Info("Ending application...");
        }

        #region private methods

        private static void BuildSourceClusterAndSession()
        {
            if (_sourceSession != null)
                return;

            Logger.Info("Building source cluster and connecting session...");

            _sourceCluster = Cluster.Builder()
                .WithPort(Config.SourcePort)
                .AddContactPoints(Config.SourceEndPoints)
                .Build();

            _sourceSession = _sourceCluster.Connect();
        }

        private static void BuildTargetClusterAndSession()
        {
            if (_targetSession != null)
                return;

            Logger.Info("Building target cluster and connecting session...");

            _targetCluster = Cluster.Builder()
                .WithPort(Config.TargetPort)
                .WithRetryPolicy(new RetryPolicy())
                .WithPoolingOptions(PoolingOptions.Create())
                .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0))
                .AddContactPoints(Config.TargetEndPoints)
                .Build();

            _targetSession = _targetCluster.Connect();
        }

        private static void ExtractionPhase()
        {
            Logger.Info("Starting extraction phase...");

            try
            {
                BuildSourceClusterAndSession();

                Logger.Info("Getting rows from source table...");

                string cql = $"SELECT * FROM {Config.SourceKeyspace}.{Config.SourceTable}";
                var statement = new SimpleStatement(cql);
                RowSet results = _sourceSession.Execute(statement);

                _ = Directory.CreateDirectory(Path.GetDirectoryName(Config.FilePath));
                using var fileWriter = new StreamWriter(Config.FilePath);

                foreach (Row result in results)
                {
                    CField[] row = new CField[result.Length];

                    for (int i = 0; i < result.Length; i++)
                    {
                        row[i] = results.Columns[i].Type.IsAssignableFrom(typeof(DateTimeOffset))
                            ? new CField(((DateTimeOffset)result[i]).ToUnixTimeMilliseconds(), results.Columns[i].Name, typeof(long))
                            : new CField(result[i], results.Columns[i].Name, results.Columns[i].Type);
                    }

                    string rowToWrite = PrepareRowToBeWritten(row);

                    fileWriter.WriteLine(rowToWrite);
                }
            }
            catch (AggregateException aggEx)
            {
                foreach (Exception ex in aggEx.Flatten().InnerExceptions)
                    Logger.Error(ex);
            }
            catch (Exception ex)
            {
                Logger.Info(ex);
            }
            finally
            {
                DisposeSourceSessionAndCluster();
            }
        }

        private static string PrepareRowToBeWritten(CField[] row)
        {
            var rowToWrite = new List<string>(row.Length);

            foreach (CField cfield in row)
            {
                string valueToWrite = Convert.ToString(cfield.Value);

                if (cfield.DataType == typeof(string) && !string.IsNullOrEmpty(valueToWrite))
                    valueToWrite = $"\"{valueToWrite.Replace("\"", "\"\"")}\"";

                rowToWrite.Add(valueToWrite);
            }

            return string.Join(',', rowToWrite);
        }

        private static void InsertionPhase()
        {
            Logger.Info("Starting insertion phase...");

            try
            {
                IList<string[]> tableData = ReadFromFile(Config.FilePath);

                BuildTargetClusterAndSession();

                IList<CColumn> columns = GetColumnsInfo(Config.TargetKeyspace, Config.TargetTable);
                InsertDataIntoTable(ref tableData, ref columns);
            }
            catch (AggregateException aggEx)
            {
                foreach (Exception ex in aggEx.Flatten().InnerExceptions)
                    Logger.Error(ex);
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
            }
            finally
            {
                DisposeTargetSessionAndCluster();
            }
        }

        private static IList<string[]> ReadFromFile(string filePath)
        {
            Logger.Info("Reading data from file...");

            if (!File.Exists(filePath))
                throw new FileNotFoundException("The file either does not exist or there is a lack of permissions to read it. Check the path provided.");

            var tableData = new List<string[]>();

            using (TextReader reader = new StreamReader(filePath))
            using (var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                ConfigureCsvReader(csvReader);

                var records = csvReader.GetRecords<dynamic>();

                foreach (IDictionary<string, object> record in records)
                {
                    var row = new List<string>(record.Values.Count);
                    row.AddRange(record.Values.Cast<string>());

                    tableData.Add(row.ToArray());
                }
            }

            Logger.Info($"Rows retrieved: {tableData.Count}");

            return tableData;
        }

        private static void ConfigureCsvReader(CsvReader csvReader)
        {
            csvReader.Configuration.Delimiter = ",";
            csvReader.Configuration.HasHeaderRecord = false;
            csvReader.Configuration.MissingFieldFound = null;
        }

        private static IList<CColumn> GetColumnsInfo(string keyspace, string table)
        {
            Logger.Info($"Getting columns info: [table] {table} [keyspace] {keyspace}");

            string cql = $"SELECT * FROM {keyspace}.{table} LIMIT 1";
            var statement = new SimpleStatement(cql);
            RowSet results = _targetSession.Execute(statement);

            return results.Columns.Select(column => new CColumn(column.Name, column.Type)).ToList();
        }

        private static void InsertDataIntoTable(ref IList<string[]> tableData, ref IList<CColumn> columns)
        {
            Logger.Info("Inserting data into target table...");

            string columnsAsString = string.Join(',', columns.GroupBy(c => c.Name).Select(c => c.Key));
            string valuesPlaceholders = string.Concat(Enumerable.Repeat("?,", columns.Count)).TrimEnd(',');

            string cql = $"INSERT INTO {Config.TargetKeyspace}.{Config.TargetTable} ({columnsAsString}) VALUES ({valuesPlaceholders})";
            PreparedStatement pStatement = _targetSession.Prepare(cql);

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

                tasks.Enqueue(_targetSession.ExecuteAsync(stmt));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            stopwatch.Stop();
            Logger.Info($"Elapsed insertion time: {stopwatch.ElapsedMilliseconds} ms");
        }

        private static bool IsRequestsLimitReached()
        {
            return CurrentInFlightQueries() >= MaxRequestsPerConnection();
        }

        private static int CurrentInFlightQueries()
        {
            ISessionState state = _targetSession.GetState();

            return state.GetConnectedHosts().Sum(host => state.GetInFlightQueries(host));
        }

        private static int MaxRequestsPerConnection()
        {
            return _targetSession.Cluster.Configuration.PoolingOptions.GetMaxRequestsPerConnection();
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
            IList<CColumn> sourceColumns = GetColumnsInfo(Config.SourceKeyspace, Config.SourceTable);
            IList<CColumn> targetColumns = GetColumnsInfo(Config.TargetKeyspace, Config.TargetTable);

            if (sourceColumns.Count != targetColumns.Count)
            {
                Logger.Error("Tables from source and target have divergent number of columns.");
                return false;
            }

            var matches = new List<bool>();

            foreach (CColumn sourceColumn in sourceColumns)
            {
                foreach (CColumn targetColumn in targetColumns)
                {
                    if (sourceColumn.Name.Equals(targetColumn.Name)
                        && sourceColumn.DataType == targetColumn.DataType)
                    {
                        matches.Add(true);
                    }
                }
            }

            if (matches.Count == sourceColumns.Count)
            {
                Logger.Info("Tables are compliant with each other.");
                return true;
            }

            Logger.Error($"Tables are not compliant with each other: {sourceColumns.Count - matches.Count} mismatch(es) among {sourceColumns.Count} columns.");

            return false;
        }

        private static void DisposeSourceSessionAndCluster()
        {
            if (_sourceSession == null || _sourceSession.IsDisposed)
                return;

            Logger.Info("Disposing source's cluster and session...");

            _sourceSession.Dispose();
            _sourceCluster.Dispose();
        }

        private static void DisposeTargetSessionAndCluster()
        {
            if (_targetSession == null || _targetSession.IsDisposed)
                return;

            Logger.Info("Disposing target's cluster and session...");

            _targetSession.Dispose();
            _targetCluster.Dispose();
        }

        #endregion
    }
}