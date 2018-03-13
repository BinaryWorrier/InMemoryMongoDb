using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using TinyIoC;

namespace InMemoryMongoDb
{
    public class InMemoryClient : IMongoClient
    {
        private static readonly TinyIoCContainer iocContainer;

        static InMemoryClient()
        {
            iocContainer = new TinyIoCContainer();
            iocContainer.RunInstallers();
        }
        private ConcurrentDictionary<string, InMemoryDatabase> databases = new ConcurrentDictionary<string, InMemoryDatabase>();

        private MongoClientSettings settings = new MongoClientSettings();

        public ICluster Cluster => null;

        public MongoClientSettings Settings => settings;

        public void DropDatabase(string name, CancellationToken cancellationToken = default)
        {
            databases.TryRemove(name, out var db); 
        }

        public void DropDatabase(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
        {
            DropDatabase(name, cancellationToken);
        }

        public Task DropDatabaseAsync(string name, CancellationToken cancellationToken = default)
        {
            DropDatabase(name, cancellationToken);
            return Task.CompletedTask;
        }

        public Task DropDatabaseAsync(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
        {
            DropDatabase(name, cancellationToken);
            return Task.CompletedTask;
        }

        public IMongoDatabase GetDatabase(string name, MongoDatabaseSettings settings = null)
            => databases.GetOrAdd(name, n => new InMemoryDatabase(this, n, iocContainer));
        

        public IAsyncCursor<BsonDocument> ListDatabases(CancellationToken cancellationToken = default)
        {
            return INMAsyncCursor.Create(databases.Keys.Select(n => (new { Name = n }).ToBsonDocument()));
        }

        public IAsyncCursor<BsonDocument> ListDatabases(IClientSessionHandle session, CancellationToken cancellationToken = default)
        {
            return ListDatabases(cancellationToken);
        }

        public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(ListDatabases(cancellationToken));
        }

        public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(IClientSessionHandle session, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(ListDatabases(cancellationToken));
        }

        public IClientSessionHandle StartSession(ClientSessionOptions options = null, CancellationToken cancellationToken = default)
        {
            return new INMClientSessionHandle(this);
        }

        public Task<IClientSessionHandle> StartSessionAsync(ClientSessionOptions options = null, CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IClientSessionHandle>(new INMClientSessionHandle(this));
        }

        public IMongoClient WithReadConcern(ReadConcern readConcern)
        {
            return this;
        }

        public IMongoClient WithReadPreference(ReadPreference readPreference)
        {
            return this;
        }

        public IMongoClient WithWriteConcern(WriteConcern writeConcern)
        {
            return this;
        }
    }
}
