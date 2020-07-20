using Cassandra;
using Microsoft.Extensions.Logging;
using Mycenae.Aspects;
using Mycenae.Models;
using Mycenae.Policies;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Mycenae.Tasks
{
    internal abstract class MigrationTask
    {
        private static ILogger<MigrationTask> _logger;

        #region Clusters & sessions
        private static ICluster _sourceCluster, _targetCluster;
        private static ISession _sourceSession, _targetSession;
        #endregion

        protected MigrationTask(ILogger<MigrationTask> logger)
        {
            _logger = logger;
        }

        internal abstract void Execute();

        protected static void BuildSourceClusterAndSession()
        {
            if (_sourceSession != null)
                return;

            _logger.LogInformation("Building source cluster and connecting session...");

            _sourceCluster = Cluster.Builder()
                .WithPort(Settings.Values.Connections.Source.Port)
                .AddContactPoints(Settings.Values.Connections.Source.Endpoints)
                .Build();

            _sourceSession = _sourceCluster.Connect();
        }

        protected static void BuildTargetClusterAndSession()
        {
            if (_targetSession != null)
                return;

            _logger.LogInformation("Building target cluster and connecting session...");

            _targetCluster = Cluster.Builder()
                .WithPort(Settings.Values.Connections.Target.Port)
                .WithRetryPolicy(new RetryPolicy())
                .WithPoolingOptions(PoolingOptions.Create())
                .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0))
                .AddContactPoints(Settings.Values.Connections.Target.Endpoints)
                .Build();

            _targetSession = _targetCluster.Connect();
        }

        protected static void DisposeSourceSessionAndCluster()
        {
            if (_sourceSession == null || _sourceSession.IsDisposed)
                return;

            _logger.LogInformation("Disposing source's cluster and session...");

            _sourceSession.Dispose();
            _sourceCluster.Dispose();
        }

        protected static void DisposeTargetSessionAndCluster()
        {
            if (_targetSession == null || _targetSession.IsDisposed)
                return;

            _logger.LogInformation("Disposing target's cluster and session...");

            _targetSession.Dispose();
            _targetCluster.Dispose();
        }

        protected static RowSet RetrieveRowsFromTable()
        {
            _logger.LogInformation("Retrieving rows from table...");

            string cql = $"SELECT * FROM {Settings.Values.Connections.Source.Keyspace}.{Settings.Values.Connections.Source.Table}";
            var statement = new SimpleStatement(cql);

            return _sourceSession.Execute(statement);
        }

        protected static PreparedStatement PrepareStatementForInsertion(IList<CColumn> columns = null)
        {
            columns ??= GetColumnsInfo(Settings.Values.Connections.Target.Keyspace, Settings.Values.Connections.Target.Table);

            string columnsAsString = string.Join(',', columns.GroupBy(c => c.Name).Select(c => c.Key));
            string valuesPlaceholders = string.Concat(Enumerable.Repeat("?,", columns.Count)).TrimEnd(',');

            string cql = $"INSERT INTO {Settings.Values.Connections.Target.Keyspace}.{Settings.Values.Connections.Target.Table} " +
                         $"({columnsAsString}) VALUES ({valuesPlaceholders})";

            return _targetSession.Prepare(cql);
        }

        protected static IList<CColumn> GetColumnsInfo(string keyspace, string table)
        {
            _logger.LogInformation($"Getting columns info: [table] {table} [keyspace] {keyspace}");

            string cql = $"SELECT * FROM {keyspace}.{table} LIMIT 1";
            var statement = new SimpleStatement(cql);
            RowSet results = _targetSession.Execute(statement);

            return results.Columns.Select(column => new CColumn(column.Name, column.Type)).ToList();
        }

        [ExecutionTimeMeasured]
        protected static async Task ExecuteInsertAsync(IList<BoundStatement> insertStatements)
        {
            _logger.LogInformation($"Inserting {insertStatements.Count} records into table...");

            var tasks = new ConcurrentQueue<Task>();

            foreach (var stmt in insertStatements)
            {
                while (IsRequestsLimitReached())
                    await Task.Delay(10);

                tasks.Enqueue(_targetSession.ExecuteAsync(stmt));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
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
    }
}
