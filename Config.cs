using Microsoft.Extensions.Configuration;
using System.IO;

namespace Mycenae
{
    public static class Config
    {
        public static IConfigurationSection TaskToPerform => Configuration.GetSection("TaskToPerform");
        public static string[] SourceEndPoints => Configuration.GetSection("SourceEndpoints").Value.Split(',');
        public static int SourcePort => int.TryParse(Configuration.GetSection("SourcePort").Value, out int sourcePort) ? sourcePort : Cassandra.ProtocolOptions.DefaultPort;
        public static string SourceKeyspace => Configuration.GetSection("SourceKeyspace").Value;
        public static string SourceTable => Configuration.GetSection("SourceTable").Value;
        public static string[] TargetEndPoints => Configuration.GetSection("TargetEndpoints").Value.Split(',');
        public static int TargetPort => int.TryParse(Configuration.GetSection("TargetPort").Value, out int targetPort) ? targetPort : Cassandra.ProtocolOptions.DefaultPort;
        public static string TargetKeyspace => Configuration.GetSection("TargetKeyspace").Value;
        public static string TargetTable => Configuration.GetSection("TargetTable").Value;
        public static string ExtractionFilePath => Configuration.GetSection("ExtractionFilePath").Value;
        public static string InsertionFilePath => Configuration.GetSection("InsertionFilePath").Value;
        public static bool InsertionFileHasHeader => string.Compare(Configuration.GetSection("InsertionFileHasHeader").Value, "true", true) == 0;
        public static int InsertionBatch => int.TryParse(Configuration.GetSection("InsertionBatch").Value, out int insertionBatch) ? insertionBatch : 100000;

        private static readonly IConfigurationRoot Configuration = new ConfigurationBuilder()
                                                                       .SetBasePath(Directory.GetCurrentDirectory())
                                                                       .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                                                                       .Build();
    }
}