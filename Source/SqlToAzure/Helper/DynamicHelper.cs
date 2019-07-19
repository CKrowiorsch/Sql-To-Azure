using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;

namespace SqlToAzure.Helper
{
    static class DynamicHelper
    {

        public static Dictionary<string, object> ToDictionary(dynamic dynObj)
        {
            var dictionary = new Dictionary<string, object>();
            var props = TypeDescriptor.GetProperties(dynObj);
            var knownColumns = new List<string>();

            foreach (PropertyDescriptor propertyDescriptor in props)
            {
                if (knownColumns.Contains(propertyDescriptor.Name, StringComparer.OrdinalIgnoreCase))
                    continue;

                object obj = propertyDescriptor.GetValue(dynObj);
                dictionary.Add(propertyDescriptor.Name, obj);

                knownColumns.Add(propertyDescriptor.Name);
            }

            return dictionary;
        }
    }
}
