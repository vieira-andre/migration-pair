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
        }
    }
}
