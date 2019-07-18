using migration_pair.Models;
using System;
using System.Configuration;
using System.IO;

namespace migration_pair.Helpers
{
    internal static class Log
    {
        private const string dateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffzzz";
        private static readonly string logFilePath = ConfigurationManager.AppSettings["Log_File_Path"];
        private static bool isFirstLog = true;

        internal static void Write(string message)
        {
            _ = Directory.CreateDirectory(Path.GetDirectoryName(logFilePath));

            File.AppendAllText(logFilePath, string.Concat(
                isFirstLog ? Environment.NewLine : null,
                DateTime.Now.ToString(dateTimeFormat), " >> ", message, Environment.NewLine));

            if (isFirstLog) isFirstLog = false;
        }

        internal static void Write(TaskToPerform procedure, ConfigurableValues config)
        {
            Write($"Task to perform: {procedure}");

            string extractMsg = string.Join(' ',
                $"[source_endpoints] {string.Join(',', config.SourceEndPoints)}",
                $"[source_keyspace] {config.SourceKeyspace}",
                $"[source_table] {config.SourceTable}");

            string insertMsg = string.Join(' ',
                $"[target_endpoints] {string.Join(',', config.TargetEndPoints)}",
                $"[target_keyspace] {config.TargetKeyspace}",
                $"[target_table] {config.TargetTable}");

            switch (procedure)
            {
                case TaskToPerform.Extract:
                    Write(extractMsg);
                    break;

                case TaskToPerform.Insert:
                    Write(insertMsg);
                    break;

                case TaskToPerform.ExtractAndInsert:
                    Write(extractMsg);
                    Write(insertMsg);
                    break;

                default:
                    break;
            }
        }
    }
}