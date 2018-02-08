using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace InMemoryMongoDb
{
    class INMDeleteResult : DeleteResult
    {
        private readonly int count;
        private readonly bool isAcked;

        public INMDeleteResult(int count, bool isAcked)
        {
            this.count = count;
            this.isAcked = isAcked;
        }
        public override long DeletedCount => count;

        public override bool IsAcknowledged => isAcked;
    }
}
