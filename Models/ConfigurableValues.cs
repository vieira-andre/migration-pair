using System.Configuration;

namespace migration_pair.Models
{
    internal class ConfigurableValues
    {
        public string TaskToPerform { get; private set; }
        public string FilePath { get; private set; }
        public string[] SourceEndPoints { get; private set; }
        public string SourceKeyspace { get; private set; }
        public string SourceTableName { get; private set; }
        public string[] TargetEndPoints { get; private set; }
        public string TargetKeyspace { get; private set; }
        public string TargetTableName { get; private set; }

        public ConfigurableValues()
        {
            TaskToPerform = ConfigurationManager.AppSettings["Task_To_Perform"];
            FilePath = ConfigurationManager.AppSettings["File_Path"];
            SourceEndPoints = ConfigurationManager.AppSettings["Source_Endpoints"].Split(',');
            SourceKeyspace = ConfigurationManager.AppSettings["Source_Keyspace"];
            SourceTableName = ConfigurationManager.AppSettings["Source_Table_Name"];
            TargetEndPoints = ConfigurationManager.AppSettings["Target_Endpoints"].Split(',');
            TargetKeyspace = ConfigurationManager.AppSettings["Target_Keyspace"];
            TargetTableName = ConfigurationManager.AppSettings["Target_Table_Name"];
        }
    }
}