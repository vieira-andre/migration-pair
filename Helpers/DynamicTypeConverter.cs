using System;
using System.Collections.Generic;

namespace migration_pair.Helpers
{
    internal static class DynamicTypeConverter
    {
        private static readonly Dictionary<Type, Func<dynamic, dynamic>> converter = new Dictionary<Type, Func<dynamic, dynamic>>
            {
                { typeof(long), (dynamic value) => { return System.Convert.ToInt64(string.IsNullOrEmpty(value) ? null : value); } },
                { typeof(int), (dynamic value) => { return System.Convert.ToInt32(string.IsNullOrEmpty(value) ? null : value); } },
                { typeof(short), (dynamic value) => { return System.Convert.ToInt16(string.IsNullOrEmpty(value) ? null : value); } },
                { typeof(DateTimeOffset), (dynamic value) => { return System.Convert.ToInt64(string.IsNullOrEmpty(value) ? null : value); } },
                { typeof(bool), (dynamic value) => { return bool.Parse(value); } }
            };

        internal static dynamic Convert(dynamic fieldValue, Type columnDataType)
        {
            return converter.ContainsKey(columnDataType)
                ? converter[columnDataType].Invoke(fieldValue)
                : fieldValue;
        }
    }
}