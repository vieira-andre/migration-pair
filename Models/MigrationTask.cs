using Cassandra;
using Mycenae.Helpers;
using Mycenae.Policies;
using NLog;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Logger = NLog.Logger;

namespace Mycenae.Models
{
    internal abstract class MigrationTask
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        #region Clusters & sessions
        private static ICluster _sourceCluster, _targetCluster;
        private static ISession _sourceSession, _targetSession;
        #endregion

        internal abstract void Execute();

        protected static void BuildSourceClusterAndSession()
        {
            if (_sourceSession != null)
                return;

            Logger.Info("Building source cluster and connecting session...");

            _sourceCluster = Cluster.Builder()
                .WithPort(Config.Values.Connections.Source.Port)
                .AddContactPoints(Config.Values.Connections.Source.Endpoints)
                .Build();

            _sourceSession = _sourceCluster.Connect();
        }

        protected static void BuildTargetClusterAndSession()
        {
            if (_targetSession != null)
                return;

            Logger.Info("Building target cluster and connecting session...");

            _targetCluster = Cluster.Builder()
                .WithPort(Config.Values.Connections.Target.Port)
                .WithRetryPolicy(new RetryPolicy())
                .WithPoolingOptions(PoolingOptions.Create())
                .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0))
                .AddContactPoints(Config.Values.Connections.Target.Endpoints)
                .Build();

            _targetSession = _targetCluster.Connect();
        }

        protected static void DisposeSourceSessionAndCluster()
        {
            if (_sourceSession == null || _sourceSession.IsDisposed)
                return;

            Logger.Info("Disposing source's cluster and session...");

            _sourceSession.Dispose();
            _sourceCluster.Dispose();
        }

        protected static void DisposeTargetSessionAndCluster()
        {
            if (_targetSession == null || _targetSession.IsDisposed)
                return;

            Logger.Info("Disposing target's cluster and session...");

            _targetSession.Dispose();
            _targetCluster.Dispose();
        }

        protected static RowSet RetrieveRowsFromTable()
        {
            Logger.Info("Retrieving rows from table...");

            string cql = $"SELECT * FROM {Config.Values.Connections.Source.Keyspace}.{Config.Values.Connections.Source.Table}";
            var statement = new SimpleStatement(cql);

            return _sourceSession.Execute(statement);
        }

        protected static PreparedStatement PrepareStatementForInsertion(IList<CColumn> columns = null)
        {
            columns ??= GetColumnsInfo(Config.Values.Connections.Target.Keyspace, Config.Values.Connections.Target.Table);

            string columnsAsString = string.Join(',', columns.GroupBy(c => c.Name).Select(c => c.Key));
            string valuesPlaceholders = string.Concat(Enumerable.Repeat("?,", columns.Count)).TrimEnd(',');

            string cql = $"INSERT INTO {Config.Values.Connections.Target.Keyspace}.{Config.Values.Connections.Target.Table} " +
                         $"({columnsAsString}) VALUES ({valuesPlaceholders})";

            return _targetSession.Prepare(cql);
        }

        protected static IList<CColumn> GetColumnsInfo(string keyspace, string table)
        {
            Logger.Info($"Getting columns info: [table] {table} [keyspace] {keyspace}");

            string cql = $"SELECT * FROM {keyspace}.{table} LIMIT 1";
            var statement = new SimpleStatement(cql);
            RowSet results = _targetSession.Execute(statement);

            return results.Columns.Select(column => new CColumn(column.Name, column.Type)).ToList();
        }

        protected static async Task ExecuteInsertAsync(IList<BoundStatement> insertStatements)
        {
            var stopwatch = StopwatchManager.Start();

            Logger.Info($"Inserting {insertStatements.Count} records into table...");

            var tasks = new ConcurrentQueue<Task>();

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

            stopwatch.StopAndLog();
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
