using System;
using System.Collections.Generic;

namespace migration_pair.Helpers
{
    internal static class DynamicTypeConverter
    {
        private static readonly Dictionary<Type, Func<dynamic, dynamic>> Converter = new Dictionary<Type, Func<dynamic, dynamic>>
            {
                { typeof(long), value => System.Convert.ToInt64(string.IsNullOrEmpty(value) ? null : value)},
                { typeof(int), value => System.Convert.ToInt32(string.IsNullOrEmpty(value) ? null : value)},
                { typeof(short), value => System.Convert.ToInt16(string.IsNullOrEmpty(value) ? null : value)},
                { typeof(DateTimeOffset), value => System.Convert.ToInt64(string.IsNullOrEmpty(value) ? null : value)},
                { typeof(bool), value => bool.Parse(value)}
            };

        internal static dynamic Convert(dynamic fieldValue, Type columnDataType)
        {
            return Converter.ContainsKey(columnDataType)
                ? Converter[columnDataType].Invoke(fieldValue)
                : fieldValue;
        }
    }
}