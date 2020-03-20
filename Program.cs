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

                RowSet rows = RetrieveRowsFromTable();

                ProcessRows(rows);
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

        private static RowSet RetrieveRowsFromTable()
        {
            Logger.Info("Retrieving rows from table...");

            string cql = $"SELECT * FROM {Config.SourceKeyspace}.{Config.SourceTable}";
            var statement = new SimpleStatement(cql);

            return _sourceSession.Execute(statement);
        }

        private static void ProcessRows(RowSet rows)
        {
            Logger.Info("Processing rows...");

            _ = Directory.CreateDirectory(Path.GetDirectoryName(Config.FilePath));
            using var fileWriter = new StreamWriter(Config.FilePath);

            string columnNames = string.Join(',', rows.Columns.Select(c => c.Name));
            fileWriter.WriteLine(columnNames);

            foreach (Row row in rows)
            {
                CField[] rowFields = new CField[row.Length];

                for (int i = 0; i < row.Length; i++)
                {
                    rowFields[i] = rows.Columns[i].Type.IsAssignableFrom(typeof(DateTimeOffset))
                        ? new CField(((DateTimeOffset)row[i]).ToUnixTimeMilliseconds(), rows.Columns[i].Name, typeof(long))
                        : new CField(row[i], rows.Columns[i].Name, rows.Columns[i].Type);
                }

                string rowToWrite = PrepareRowToBeWritten(rowFields);

                fileWriter.WriteLine(rowToWrite);
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
                BuildTargetClusterAndSession();

                if (!File.Exists(Config.FilePath))
                    throw new FileNotFoundException("The file either does not exist or there is a lack of permissions to read it. Check the path provided.");

                IList<CColumn> columns = GetColumnsInfo(Config.TargetKeyspace, Config.TargetTable);

                Logger.Info("Reading data from file...");

                using var reader = new StreamReader(Config.FilePath);
                using var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture);
                ConfigureCsvReader(csvReader);

                var records = csvReader.GetRecords<dynamic>();

                string columnsAsString = string.Join(',', columns.GroupBy(c => c.Name).Select(c => c.Key));
                string valuesPlaceholders = string.Concat(Enumerable.Repeat("?,", columns.Count)).TrimEnd(',');

                string cql = $"INSERT INTO {Config.TargetKeyspace}.{Config.TargetTable} ({columnsAsString}) VALUES ({valuesPlaceholders})";
                PreparedStatement pStatement = _targetSession.Prepare(cql);

                var insertStatements = new List<BoundStatement>();

                foreach (IDictionary<string, object> record in records)
                {
                    var row = new List<string>(record.Values.Count);
                    row.AddRange(record.Values.Cast<string>());

                    dynamic[] preparedRow = PrepareRowForInsertion(columns, row);

                    BoundStatement bStatement = pStatement.Bind(preparedRow);
                    insertStatements.Add(bStatement);
                }

                Logger.Info("Inserting data into target table...");

                ExecuteInsertAsync(insertStatements).Wait();
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

        private static dynamic[] PrepareRowForInsertion(IList<CColumn> columns, List<string> row)
        {
            dynamic[] preparedRow = new dynamic[row.Count];

            for (int i = 0; i < row.Count; i++)
            {
                preparedRow[i] = DynamicTypeConverter.Convert(row[i], columns[i].DataType);
            }

            return preparedRow;
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
            csvReader.Configuration.HasHeaderRecord = true;
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