using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace SideBySideTests
{
    public interface ITest
    {
        IEnumerable<BsonDocument> Run(IMongoDatabase db, IEnumerator<Guid> guidIter);
    }

    public abstract class TestBase : ITest
    {
        public TestBase()
        {
        }
        public IEnumerable<BsonDocument> Run(IMongoDatabase db, IEnumerator<Guid> guidIter)
        {
            return RunTest(db, guidIter);
        }

        protected abstract IEnumerable<BsonDocument> RunTest(IMongoDatabase db, IEnumerator<Guid> guidIter);

        protected Guid GetGuid(IEnumerator<Guid> guidIter)
        {
            guidIter.MoveNext();
            return guidIter.Current;
        }

        protected IEnumerable<BsonDocument> GetBson(IMongoDatabase db, string colName)
        {
            var col = db.GetCollection<BsonDocument>(colName);

            var docs = col.Find(_ => true).ToEnumerable();

            foreach(var doc in docs)
            {
                doc.Remove("_id");
                yield return doc;
            }
        }

    }
}
