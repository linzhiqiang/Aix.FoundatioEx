using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.FoundatioEx.Kafka.Extensions
{
    internal static class DictionaryExtensions
    {
        public static void AddOrUpdate<K, V>(this IDictionary<K, V> dict, K key, V value)
        {
            if (dict.ContainsKey(key))
            {
                dict[key] = value;
            }
            else
            {
                dict.Add(key, value);
            }
        }

        public static V GetValue<K, V>(this IDictionary<K, V> dict, K key)
        {
            if (dict.ContainsKey(key))
            {
                return dict[key];
            }
            return default(V);
        }

        public static V GetValue<K, V>(this IDictionary<K, V> dict, params K[] keys)
        {
            foreach (var item in keys)
            {
                if (dict.ContainsKey(item))
                {
                    return dict[item];
                }
            }

            return default(V);
        }


    }
}
