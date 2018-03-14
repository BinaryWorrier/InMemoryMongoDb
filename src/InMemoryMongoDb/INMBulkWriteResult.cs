using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace InMemoryMongoDb
{
    class INMBulkWriteResult<T> : BulkWriteResult<T>
    {
        public long INMDeletedCount { get; set; }
        public long INMInsertedCount { get; set; }
        public long INMMatchedCount { get; set; }
        public long INMModifiedCount { get; set; }

        public List<BulkWriteUpsert> IMNUpserts { get; set; } 
        public INMBulkWriteResult(int requestCount, IEnumerable<WriteModel<T>> processedRequests)
            :base(requestCount, processedRequests)
        {
            //IMNUpserts.Add(new BulkWriteUpsert())
        }


        public override long DeletedCount => INMDeletedCount;

        public override long InsertedCount => INMInsertedCount;

        public override bool IsAcknowledged => true;

        public override bool IsModifiedCountAvailable => true;

        public override long MatchedCount => INMMatchedCount;

        public override long ModifiedCount => INMModifiedCount;

        public override IReadOnlyList<BulkWriteUpsert> Upserts => IMNUpserts.AsReadOnly();
    }

    public class INMBulkWriteUpsert
    {
        public static BulkWriteUpsert Create(int index, BsonValue id)
        {
            return creator(index, id);
        }
        private static Func<int, BsonValue, BulkWriteUpsert> creator = CreateInstanceFunc();
        private static Func<int, BsonValue, BulkWriteUpsert> CreateInstanceFunc()
        {
            var flags = BindingFlags.NonPublic | BindingFlags.Instance;
            var ctor = typeof(BulkWriteUpsert).GetConstructors(flags).Single(
                ctors =>
                {
                    var parameters = ctors.GetParameters();
                    return parameters.Length == 2 && parameters[0].ParameterType == typeof(int) && parameters[1].ParameterType == typeof(BsonValue);
                });
            var index = Expression.Parameter(typeof(int), "index");
            var id = Expression.Parameter(typeof(BsonValue), "id");
            var body = Expression.New(ctor, index, id);
            var lambda = Expression.Lambda<Func<int, BsonValue, BulkWriteUpsert>>(body, index, id);

            return lambda.Compile();
        }
    }

}
