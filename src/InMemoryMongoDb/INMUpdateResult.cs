using MongoDB.Bson;
using MongoDB.Driver;

namespace InMemoryMongoDb
{
    internal class INMUpdateResult : UpdateResult
    {
        public INMUpdateResult(bool isAcknowledged, bool isModifiedCountAvailable, long matchedCount, long modifiedCount, BsonValue upsertedId)
        {
            IsAcknowledged = isAcknowledged;
        }
        public override bool IsAcknowledged { get; }

        public override bool IsModifiedCountAvailable { get; }

        public override long MatchedCount { get; }

        public override long ModifiedCount { get; }

        public override BsonValue UpsertedId { get; }
    }
}