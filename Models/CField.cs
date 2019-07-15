using System;

namespace migration_pair
{
    internal class CField
    {
        public dynamic Value { get; set; }
        public string ColumnName { get; set; }
        public Type DataType { get; set; }

        public CField(dynamic value, string columnName, Type dataType)
        {
            Value = value;
            ColumnName = columnName;
            DataType = dataType;
        }
    }
}