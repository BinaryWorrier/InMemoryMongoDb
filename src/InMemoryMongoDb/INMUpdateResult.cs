using MongoDB.Bson;
using MongoDB.Driver;

namespace InMemoryMongoDb
{
    internal class INMUpdateResult : UpdateResult
    {
        public INMUpdateResult(bool isAcknowledged, long matchedCount, long modifiedCount, BsonValue upsertedId)
        {
            IsAcknowledged = isAcknowledged;
            MatchedCount = matchedCount;
            ModifiedCount = modifiedCount;
            UpsertedId = upsertedId;
        }

        public override bool IsAcknowledged { get; }

        public override bool IsModifiedCountAvailable { get; } = true;

        public override long MatchedCount { get; }

        public override long ModifiedCount { get; }

        public override BsonValue UpsertedId { get; }
    }
}