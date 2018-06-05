using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace SideBySideTests
{
    public static class MongoDbExtensions
    {
        public static (IMongoCollection<T>, string) GetCollection<T>(this IMongoDatabase db)
            => (db.GetCollection<T>(typeof(T).Name), typeof(T).Name);

    }
}
