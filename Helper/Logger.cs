using System.Configuration;

namespace migration_pair.Helper
{
    internal static class Logger
    {
        private static readonly string logFilePath = ConfigurationManager.AppSettings["Log_File_Path"];

        internal async static void Write(string message)
        {

        }
    }
}
