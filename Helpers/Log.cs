using migration_pair.Models;
using System;
using System.IO;

namespace migration_pair.Helpers
{
    internal class Log
    {
        private const string dateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffzzz";
        private readonly string logFilePath;
        private bool isFirstLog = true;

        public Log(string logFilePath)
        {
            this.logFilePath = logFilePath;
        }

        internal void Write(string message)
        {
            _ = Directory.CreateDirectory(Path.GetDirectoryName(logFilePath));

            File.AppendAllText(logFilePath, string.Concat(
                isFirstLog ? Environment.NewLine : null,
                DateTime.Now.ToString(dateTimeFormat), " >> ", message, Environment.NewLine));

            if (isFirstLog) isFirstLog = false;
        }

        internal void Write(TaskToPerform procedure, ConfigurableValues config)
        {
            Write($"Task to perform: {procedure}");

            string extractMsg = string.Join(' ', "Source info:", Environment.NewLine,
                $"[source_endpoints] {string.Join(',', config.SourceEndPoints)}", Environment.NewLine,
                $"[source_port] {config.SourcePort}", Environment.NewLine,
                $"[source_keyspace] {config.SourceKeyspace}", Environment.NewLine,
                $"[source_table] {config.SourceTable}");

            string insertMsg = string.Join(' ', "Target info:", Environment.NewLine,
                $"[target_endpoints] {string.Join(',', config.TargetEndPoints)}", Environment.NewLine,
                $"[target_port] {config.TargetPort}", Environment.NewLine,
                $"[target_keyspace] {config.TargetKeyspace}", Environment.NewLine,
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