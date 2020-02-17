using migration_pair.Models;
using System;
using System.IO;

namespace migration_pair.Helpers
{
    internal class Log
    {
        private const string DateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffzzz";
        private readonly string _logFilePath;
        private static bool _isFirstLog = true;

        public Log(string logFilePath)
        {
            _logFilePath = logFilePath;
        }

        internal void Write(string message)
        {
            _ = Directory.CreateDirectory(Path.GetDirectoryName(_logFilePath));

            File.AppendAllText(_logFilePath, string.Concat(
                _isFirstLog ? Environment.NewLine : null,
                DateTime.Now.ToString(DateTimeFormat), " >> ", message, Environment.NewLine));

            if (_isFirstLog) _isFirstLog = false;
        }

        internal void Write(TaskToPerform procedure)
        {
            Write($"Task to perform: {procedure}");

            string extractMsg = string.Join(' ', "Source info:", Environment.NewLine,
                $"[source_endpoints] {string.Join(',', Config.SourceEndPoints)}", Environment.NewLine,
                $"[source_port] {Config.SourcePort}", Environment.NewLine,
                $"[source_keyspace] {Config.SourceKeyspace}", Environment.NewLine,
                $"[source_table] {Config.SourceTable}");

            string insertMsg = string.Join(' ', "Target info:", Environment.NewLine,
                $"[target_endpoints] {string.Join(',', Config.TargetEndPoints)}", Environment.NewLine,
                $"[target_port] {Config.TargetPort}", Environment.NewLine,
                $"[target_keyspace] {Config.TargetKeyspace}", Environment.NewLine,
                $"[target_table] {Config.TargetTable}");

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