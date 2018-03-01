using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace InMemoryMongoDb
{
    class INMClientSessionHandle : IClientSessionHandle
    {
        public INMClientSessionHandle(IMongoClient client)
        {
            Client = client;
            Options = new ClientSessionOptions { };
        }

        public IMongoClient Client { get; private set; }

        public BsonDocument ClusterTime { get; private set; }

        public bool IsImplicit => true;

        public BsonTimestamp OperationTime { get; private set; }

        public ClientSessionOptions Options { get; private set; }

        public IServerSession ServerSession => throw new NotImplementedException();

        public void AdvanceClusterTime(BsonDocument newClusterTime)
        {
            ClusterTime = newClusterTime;
        }

        public void AdvanceOperationTime(BsonTimestamp newOperationTime)
        {
            OperationTime = newOperationTime;
        }

        public void Dispose()
        {
            
        }

        public IClientSessionHandle Fork()
        {
            return this;
        }
    }
}
