using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InMemoryMongoDb
{
    static class MyLinq
    {
        public static IEnumerable<R> DistinctBy<T, R>(this IEnumerable<T> items, Func<T, R> accessor)
            =>new HashSet<R>(items.Select(accessor));
    }
}
