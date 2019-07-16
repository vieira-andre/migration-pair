using System.Collections.Generic;

namespace migration_pair.Models
{
    internal class CTable
    {
        public string Name { get; set; }
        public string Keyspace { get; set; }
        public List<CField[]> Rows { get; set; }

        public CTable(string name, string keyspace)
        {
            Name = name;
            Keyspace = keyspace;
            Rows = new List<CField[]>();
        }
    }
}