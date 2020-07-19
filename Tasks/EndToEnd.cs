using Cassandra;
using Mycenae.Attributes;
using Mycenae.Models;
using NLog;
using System;
using System.Collections.Generic;
using Logger = NLog.Logger;

namespace Mycenae.Tasks
{
    internal class EndToEnd : MigrationTask
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        [ExecutionTimeMeasured]
        internal override void Execute()
        {
            Logger.Info("Starting end-to-end migration...");

            try
            {
                BuildSourceClusterAndSession();
                BuildTargetClusterAndSession();

                if (!IsThereCompliance())
                    return;

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
                Logger.Error(ex);
            }
            finally
            {
                DisposeSourceSessionAndCluster();
                DisposeTargetSessionAndCluster();
            }
        }

        private static bool IsThereCompliance()
        {
            IList<CColumn> sourceColumns = GetColumnsInfo(Settings.Values.Connections.Source.Keyspace, Settings.Values.Connections.Source.Table);
            IList<CColumn> targetColumns = GetColumnsInfo(Settings.Values.Connections.Target.Keyspace, Settings.Values.Connections.Target.Table);

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

        private static void ProcessRows(RowSet rows)
        {
            PreparedStatement pStatement = PrepareStatementForInsertion();
            var insertStatements = new List<BoundStatement>();

            foreach (Row row in rows)
            {
                var rowFields = new dynamic[row.Length];

                for (int i = 0; i < row.Length; i++)
                    rowFields[i] = row[i];

                BoundStatement bStatement = pStatement.Bind(rowFields);
                insertStatements.Add(bStatement);

                if (insertStatements.Count >= Settings.Values.InsertionBatch)
                {
                    ExecuteInsertAsync(insertStatements).Wait();
                    insertStatements.Clear();
                }
            }

            if (insertStatements.Count > 0)
                ExecuteInsertAsync(insertStatements).Wait();
        }
    }
}
