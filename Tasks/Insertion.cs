﻿using Cassandra;
using CsvHelper;
using Mycenae.Attributes;
using Mycenae.Converters;
using Mycenae.Models;
using NLog;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Logger = NLog.Logger;

namespace Mycenae.Tasks
{
    internal class Insertion : MigrationTask
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        [ExecutionTimeMeasured]
        internal override void Execute()
        {
            Logger.Info("Starting insertion phase...");

            try
            {
                BuildTargetClusterAndSession();

                if (!File.Exists(Settings.Values.Files.Insertion.Path))
                    throw new FileNotFoundException("The file either does not exist or there is a lack of permissions to read it. Check the path provided.");

                IEnumerable<dynamic> records = ReadRecordsFromFile();
                ProcessRecords(records);
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

        private static IEnumerable<dynamic> ReadRecordsFromFile()
        {
            Logger.Info("Reading data from file...");

            var reader = new StreamReader(Settings.Values.Files.Insertion.Path);
            var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture);

            ConfigureCsvReader(csvReader);

            return csvReader.GetRecords<dynamic>();
        }

        private static void ProcessRecords(IEnumerable<dynamic> records)
        {
            Logger.Info("Processing records...");

            IList<CColumn> columns = GetColumnsInfo(Settings.Values.Connections.Target.Keyspace, Settings.Values.Connections.Target.Table);
            PreparedStatement pStatement = PrepareStatementForInsertion(columns);

            var insertStatements = new List<BoundStatement>();

            foreach (IDictionary<string, object> record in records)
            {
                var row = new List<string>(record.Values.Count);
                row.AddRange(record.Values.Cast<string>());

                dynamic[] preparedRow = PrepareRowForInsertion(columns, row);

                BoundStatement bStatement = pStatement.Bind(preparedRow);

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

        private static dynamic[] PrepareRowForInsertion(IList<CColumn> columns, List<string> row)
        {
            dynamic[] preparedRow = new dynamic[row.Count];

            for (int i = 0; i < row.Count; i++)
            {
                preparedRow[i] = DynamicTypeConverter.Convert(row[i], columns[i].DataType);
            }

            return preparedRow;
        }

        private static void ConfigureCsvReader(CsvReader csvReader)
        {
            csvReader.Configuration.Delimiter = Settings.Values.Files.Insertion.Delimiter;
            csvReader.Configuration.HasHeaderRecord = Settings.Values.Files.Insertion.HasHeader;
            csvReader.Configuration.MissingFieldFound = null;
        }
    }
}
