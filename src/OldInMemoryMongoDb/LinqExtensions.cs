using System;
using System.Collections.Generic;
using System.Text;

namespace InMemoryMongoDb
{
    internal static class LinqExtensions
    {
        public static IEnumerable<T> TakeAllButLast<T>(this IEnumerable<T> items)
        {
            using (var iter = items.GetEnumerator())
            {
                if(iter.MoveNext())
                {
                    var last = iter.Current;
                    while(iter.MoveNext())
                    {
                        yield return last;
                        last = iter.Current;
                    }
                }
            }
        }
    }
}
