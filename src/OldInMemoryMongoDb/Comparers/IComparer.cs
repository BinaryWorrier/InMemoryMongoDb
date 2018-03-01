using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Text;

namespace InMemoryMongoDb.Comparers
{
    internal interface IComparer
    {
        bool Compare(BsonValue lhs, BsonValue rhs);
    }
}
