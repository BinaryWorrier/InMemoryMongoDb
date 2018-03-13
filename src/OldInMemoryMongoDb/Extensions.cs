using System;
using System.Collections.Generic;
using System.Text;

namespace InMemoryMongoDb
{
    static class Extensions
    {
        public static V GetOrDefault<K,V>(this IDictionary<K,V> dict, K key)
        {
            if (dict.TryGetValue(key, out var val))
                return val;
            return default;
        }
    }
}
