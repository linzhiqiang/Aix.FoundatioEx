using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Aix.FoundatioEx.Kafka.Utils
{
    public static class AttributeUtils
    {
        public static T GetAttribute<T>(Type type) where T : Attribute
        {
            var attrs = type.GetCustomAttributes(typeof(T), true);
            return attrs != null && attrs.Length > 0 ? attrs[0] as T : null;
        }

        public static List<T> GetAttributes<T>(Type type) where T : Attribute
        {
            List<T> result = new List<T>();
            var attrs = type.GetCustomAttributes(typeof(T), true);
            if (attrs != null)
            {
                foreach (var item in attrs)
                {
                    result.Add(item as T);
                }
            }
            return result;
        }

        public static object GetPropertyValue<TAttribute>(object message) where TAttribute : Attribute
        {
            if (message == null) return null;
            foreach (PropertyInfo item in message.GetType().GetProperties())
            {
                var attr = item.GetCustomAttribute<TAttribute>(true);
                if (attr != null)
                {
                    return item.GetValue(message);
                }
            }
            return null;
        }
    }
}
