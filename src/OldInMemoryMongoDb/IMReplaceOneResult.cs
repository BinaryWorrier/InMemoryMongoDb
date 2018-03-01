using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace InMemoryMongoDb
{
    class IMReplaceOneResult : ReplaceOneResult
    {
        public IMReplaceOneResult(bool isAcknowledged, bool isModifiedCountAvailable, long matchedCount, long modifiedCount, BsonValue upsertedId)
        {
            IsAcknowledged = isAcknowledged;
            IsModifiedCountAvailable = isModifiedCountAvailable;
            matchedCount = MatchedCount;
            modifiedCount = ModifiedCount;
            upsertedId = UpsertedId;
        }
        public override bool IsAcknowledged { get; }

        public override bool IsModifiedCountAvailable { get; }

        public override long MatchedCount { get; }

        public override long ModifiedCount { get; }

        public override BsonValue UpsertedId { get; }
    }
}
