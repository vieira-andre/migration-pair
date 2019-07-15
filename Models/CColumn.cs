using System;

namespace migration_pair
{
    internal class CColumn
    {
        public string Name { get; set; }
        public Type DataType { get; set; }

        public CColumn(string name, Type dataType)
        {
            Name = name;
            DataType = dataType;
        }
    }
}