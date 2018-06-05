using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SideBySideTests
{
    public class Simple_InsertOne_Delete_Count_01 : TestBase
    {
        private readonly Assert assert;
        public Simple_InsertOne_Delete_Count_01(Assert assert)
        {
            this.assert = assert ?? throw new ArgumentNullException(nameof(assert));
        }

        protected override IEnumerable<BsonDocument> RunTest(IMongoDatabase db, IEnumerator<Guid> guidIter)
        {
            var (col, colName) = db.GetCollection<SimpleEntity>();
            assert.Equal(col.Count(_=>true), 0);

            var entity = new SimpleEntity { Int = 1, Name = "a name", TestRowId = GetGuid(guidIter) };

            col.InsertOne(entity);

            assert.NotEqual(entity.Id, ObjectId.Empty);

            assert.Equal(col.Count(_ => true), 1);

            return GetBson(db, colName);
        }
    }
}
