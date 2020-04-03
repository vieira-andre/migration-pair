using Cassandra;
using CsvHelper;
using Mycenae.Helpers;
using NLog;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Logger = NLog.Logger;

namespace Mycenae.Models
{
    internal class Insertion : MigrationTask
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        internal override void Execute()
        {
            Logger.Info("Starting insertion phase...");

            try
            {
                var stopwatch = StopwatchManager.Start();

                BuildTargetClusterAndSession();

                if (!File.Exists(Config.FilePath))
                    throw new FileNotFoundException("The file either does not exist or there is a lack of permissions to read it. Check the path provided.");

                IEnumerable<dynamic> records = ReadRecordsFromFile();
                ProcessRecords(records);

                stopwatch.StopAndLog();
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

            var reader = new StreamReader(Config.FilePath);
            var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture);

            ConfigureCsvReader(csvReader);

            return csvReader.GetRecords<dynamic>();
        }

        private static void ProcessRecords(IEnumerable<dynamic> records)
        {
            Logger.Info("Processing records...");

            IList<CColumn> columns = GetColumnsInfo(Config.TargetKeyspace, Config.TargetTable);
            PreparedStatement pStatement = PrepareStatementForInsertion(columns);

            var insertStatements = new List<BoundStatement>();

            foreach (IDictionary<string, object> record in records)
            {
                var row = new List<string>(record.Values.Count);
                row.AddRange(record.Values.Cast<string>());

                dynamic[] preparedRow = PrepareRowForInsertion(columns, row);

                BoundStatement bStatement = pStatement.Bind(preparedRow);

                insertStatements.Add(bStatement);

                if (insertStatements.Count >= Config.InsertionBatch)
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
            csvReader.Configuration.Delimiter = ",";
            csvReader.Configuration.HasHeaderRecord = true;
            csvReader.Configuration.MissingFieldFound = null;
        }
    }
}
