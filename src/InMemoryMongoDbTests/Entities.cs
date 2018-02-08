using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace InMemoryMongoDbTests
{
    class MyEntitiesThings
    {
        public string ThingA { get; set; }
        public string ThingB { get; set; }
        public string ThingC { get; set; }
    }
    [BsonDiscriminator(RootClass = true)]
    [BsonKnownTypes(typeof(MyDerivedEntity))]
    class MyEntity
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; }
        public List<string> Books { get; set; }
        public int Number { get; set; }
        public MyEntitiesThings TheThings { get; set; }
    }
    class MyDerivedEntity : MyEntity
    {
        public string ExtraStuff { get; set; }
    }
}
