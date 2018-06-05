using MongoDB.Bson;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace InMemoryMongoDb
{
    internal class VanillaCollection
    {
        public ConcurrentDictionary<object, BsonDocument> Docs { get; private set; }
        public string Name { get; private set; }

        public VanillaCollection(string name)
        {
            Name = name;
            Docs = new ConcurrentDictionary<object, BsonDocument>();
        }

    }
}
