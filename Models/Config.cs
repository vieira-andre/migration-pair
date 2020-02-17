using Microsoft.Extensions.Configuration;
using System.IO;

namespace migration_pair.Models
{
    public static class Config
    {
        public static IConfigurationSection TaskToPerform => Configuration.GetSection("TaskToPerform");
        public static string FilePath => Configuration.GetSection("FilePath").Value;
        public static string LogFilePath => Configuration.GetSection("LogFilePath").Value;
        public static string[] SourceEndPoints => Configuration.GetSection("SourceEndpoints").Value.Split(',');
        public static int SourcePort => int.TryParse(Configuration.GetSection("SourcePort").Value, out int sourcePort) ? sourcePort : Cassandra.ProtocolOptions.DefaultPort;
        public static string SourceKeyspace => Configuration.GetSection("SourceKeyspace").Value;
        public static string SourceTable => Configuration.GetSection("SourceTable").Value;
        public static string[] TargetEndPoints => Configuration.GetSection("TargetEndpoints").Value.Split(',');
        public static int TargetPort => int.TryParse(Configuration.GetSection("TargetPort").Value, out int targetPort) ? targetPort : Cassandra.ProtocolOptions.DefaultPort;
        public static string TargetKeyspace => Configuration.GetSection("TargetKeyspace").Value;
        public static string TargetTable => Configuration.GetSection("TargetTable").Value;

        private static readonly IConfigurationRoot Configuration = new ConfigurationBuilder()
                                                                        .SetBasePath(Directory.GetCurrentDirectory())
                                                                        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                                                                        .Build();
    }
}