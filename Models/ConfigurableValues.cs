using Microsoft.Extensions.Configuration;
using migration_pair.Helpers;
using System.IO;

namespace migration_pair.Models
{
    internal class ConfigurableValues
    {
        public string TaskToPerform { get; private set; }
        public string FilePath { get; private set; }
        public string[] SourceEndPoints { get; private set; }
        public int SourcePort { get; private set; }
        public string SourceKeyspace { get; private set; }
        public string SourceTable { get; private set; }
        public string[] TargetEndPoints { get; private set; }
        public int TargetPort { get; private set; }
        public string TargetKeyspace { get; private set; }
        public string TargetTable { get; private set; }

        public ConfigurableValues()
        {
            var builder = new ConfigurationBuilder()
                              .SetBasePath(Directory.GetCurrentDirectory())
                              .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            Log.Write("Assigning configurable values...");

            TaskToPerform = configuration.GetSection("TaskToPerform").Value;
            FilePath = configuration.GetSection("FilePath").Value;
            SourceEndPoints = configuration.GetSection("SourceEndpoints").Value.Split(',');
            SourcePort = int.TryParse(configuration.GetSection("SourcePort").Value, out int sourcePort) ? sourcePort : Cassandra.ProtocolOptions.DefaultPort;
            SourceKeyspace = configuration.GetSection("SourceKeyspace").Value;
            SourceTable = configuration.GetSection("SourceTable").Value;
            TargetEndPoints = configuration.GetSection("TargetEndpoints").Value.Split(',');
            TargetPort = int.TryParse(configuration.GetSection("TargetPort").Value, out int targetPort) ? targetPort : Cassandra.ProtocolOptions.DefaultPort;
            TargetKeyspace = configuration.GetSection("TargetKeyspace").Value;
            TargetTable = configuration.GetSection("TargetTable").Value;
        }
    }
}