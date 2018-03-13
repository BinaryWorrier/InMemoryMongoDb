using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace InMemoryMongoDb
{
    class INMDeleteResult : DeleteResult
    {
        public INMDeleteResult(int count, bool isAcked)
        {
            DeletedCount = count;
            IsAcknowledged = isAcked;
        }
        public override long DeletedCount { get; }

        public override bool IsAcknowledged { get; }
    }
}
