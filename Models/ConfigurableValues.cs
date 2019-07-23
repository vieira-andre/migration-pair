using migration_pair.Helpers;
using System.Configuration;

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
            Log.Write("Assigning configurable values...");

            TaskToPerform = ConfigurationManager.AppSettings["Task_To_Perform"];
            FilePath = ConfigurationManager.AppSettings["File_Path"];
            SourceEndPoints = ConfigurationManager.AppSettings["Source_Endpoints"].Split(',');
            SourcePort = int.TryParse(ConfigurationManager.AppSettings["Source_Port"], out int sourcePort) ? sourcePort : Cassandra.ProtocolOptions.DefaultPort;
            SourceKeyspace = ConfigurationManager.AppSettings["Source_Keyspace"];
            SourceTable = ConfigurationManager.AppSettings["Source_Table"];
            TargetEndPoints = ConfigurationManager.AppSettings["Target_Endpoints"].Split(',');
            TargetPort = int.TryParse(ConfigurationManager.AppSettings["Target_Port"], out int targetPort) ? targetPort : Cassandra.ProtocolOptions.DefaultPort;
            TargetKeyspace = ConfigurationManager.AppSettings["Target_Keyspace"];
            TargetTable = ConfigurationManager.AppSettings["Target_Table"];
        }
    }
}