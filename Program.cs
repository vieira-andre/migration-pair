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
        private static readonly Log _logger = new Log(Config.LogFilePath);

        #region Clusters & sessions
        private static ICluster _sourceCluster, _targetCluster;
        private static ISession _sourceSession, _targetSession;
        #endregion

        static void Main()
        {
            if (!Enum.TryParse(Config.TaskToPerform.Value, true, out TaskToPerform procedure))
                _logger.Write($"[Error] Config entry {Config.TaskToPerform.Path} is either unspecified or misspecified.");

            _logger.Write(procedure);

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

            _logger.Write("Ending application...");
        }

        private static void ConfigureNLog()
        {
            var config = new NLog.Config.LoggingConfiguration();

            using (var logFile = new NLog.Targets.FileTarget("logfile")
            {
                ArchiveOldFileOnStartup = true,
                ArchiveNumbering = NLog.Targets.ArchiveNumberingMode.DateAndSequence,
                CreateDirs = true,
                FileName = Config.LogFilePath
            })
            using (var logConsole = new NLog.Targets.ConsoleTarget("logconsole"))
            {
                config.AddRule(NLog.LogLevel.Info, NLog.LogLevel.Error, logConsole);
                config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Error, logFile);
            }

            LogManager.Configuration = config;
        }

        private static void BuildSourceClusterAndSession()
        {
            if (_sourceSession != null)
                return;

            _logger.Write("Building source cluster and connecting session...");

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

            _logger.Write("Building target cluster and connecting session...");

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
            _logger.Write("Starting extraction phase...");

            try
            {
                BuildSourceClusterAndSession();

                var ctable = new CTable(Config.SourceTable, Config.SourceKeyspace);
                GetRows(ref ctable);

                StringBuilder tableData = WriteResultsToObject(ctable);
                SaveResultsIntoFile(ref tableData, Config.FilePath);
            }
            catch (AggregateException aggEx)
            {
                foreach (Exception ex in aggEx.Flatten().InnerExceptions)
                    _logger.Write($"[Exception] {ex}");
            }
            catch (Exception ex)
            {
                _logger.Write($"[Exception] {ex}");
            }
            finally
            {
                DisposeSourceSessionAndCluster();
            }
        }

        private static void InsertionPhase()
        {
            _logger.Write("Starting insertion phase...");

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
                    _logger.Write($"[Exception] {ex}");
            }
            catch (Exception ex)
            {
                _logger.Write($"[Exception] {ex}");
            }
            finally
            {
                DisposeTargetSessionAndCluster();
            }
        }

        private static void GetRows(ref CTable ctable)
        {
            _logger.Write("Getting rows from source table...");

            string cql = $"SELECT * FROM {ctable.Keyspace}.{ctable.Name}";
            var statement = new SimpleStatement(cql);
            RowSet results = _sourceSession.Execute(statement);

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

            _logger.Write($"Rows retrieved: {ctable.Rows.Count}");
        }

        private static StringBuilder WriteResultsToObject(CTable ctable)
        {
            _logger.Write("Writing extraction results to object...");

            var tableData = new StringBuilder();

            foreach (CField[] row in ctable.Rows)
            {
                var rowToWrite = new List<string>(row.Length);

                foreach (CField cfield in row)
                {
                    string valueToWrite = Convert.ToString(cfield.Value);

                    if (cfield.DataType == typeof(string) && !string.IsNullOrEmpty(valueToWrite))
                        valueToWrite = $"\"{valueToWrite.Replace("\"", "\"\"")}\"";

                    rowToWrite.Add(valueToWrite);
                }

                tableData.AppendLine(string.Join(',', rowToWrite));
            }

            return tableData;
        }

        private static void SaveResultsIntoFile(ref StringBuilder tableData, string filePath)
        {
            _logger.Write("Saving extraction results into file...");

            _ = Directory.CreateDirectory(Path.GetDirectoryName(filePath));
            File.WriteAllText(filePath, tableData.ToString());
        }

        private static IList<string[]> ReadFromFile(string filePath)
        {
            _logger.Write("Reading data from file...");

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

            _logger.Write($"Rows retrieved: {tableData.Count}");

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
            _logger.Write($"Getting columns info: [table] {table} [keyspace] {keyspace}");

            string cql = $"SELECT * FROM {keyspace}.{table} LIMIT 1";
            var statement = new SimpleStatement(cql);
            RowSet results = _targetSession.Execute(statement);

            return results.Columns.Select(column => new CColumn(column.Name, column.Type)).ToList();
        }

        private static void InsertDataIntoTable(ref IList<string[]> tableData, ref IList<CColumn> columns)
        {
            _logger.Write("Inserting data into target table...");

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
            _logger.Write($"Elapsed insertion time: {stopwatch.ElapsedMilliseconds} ms");
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
                _logger.Write("[Error] Tables from source and target have divergent number of columns.");
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
                _logger.Write("Tables are compliant with each other.");
                return true;
            }

            _logger.Write($"Tables are not compliant with each other: {sourceColumns.Count - matches.Count} mismatch(es) among {sourceColumns.Count} columns.");

            return false;
        }

        private static void DisposeSourceSessionAndCluster()
        {
            if (_sourceSession == null || _sourceSession.IsDisposed)
                return;

            _logger.Write("Disposing source's cluster and session...");

            _sourceSession.Dispose();
            _sourceCluster.Dispose();
        }

        private static void DisposeTargetSessionAndCluster()
        {
            if (_targetSession == null || _targetSession.IsDisposed)
                return;

            _logger.Write("Disposing target's cluster and session...");

            _targetSession.Dispose();
            _targetCluster.Dispose();
        }
    }
}