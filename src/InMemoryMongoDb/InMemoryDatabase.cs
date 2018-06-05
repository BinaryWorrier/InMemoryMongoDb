using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TinyIoC;

namespace InMemoryMongoDb
{
    class InMemoryDatabase : IMongoDatabase
    {
        private readonly string name;
        private readonly TinyIoCContainer iocContainer;
        private readonly ConcurrentDictionary<string, VanillaCollection> collections = new ConcurrentDictionary<string, VanillaCollection>();

        public InMemoryDatabase(IMongoClient client, string name, TinyIoCContainer iocContainer)
        {
            Client = client ?? throw new ArgumentNullException(nameof(client));
            this.name = name ?? throw new ArgumentNullException(nameof(name));
            this.iocContainer = iocContainer ?? throw new ArgumentNullException(nameof(iocContainer));
        }

        public IMongoClient Client { get; private set; }

        public DatabaseNamespace DatabaseNamespace => new DatabaseNamespace(name);

        public MongoDatabaseSettings Settings => new MongoDatabaseSettings();

        public void CreateCollection(string name, CreateCollectionOptions options = null, CancellationToken cancellationToken = default)
        {
            collections.TryAdd(name, null);
        }

        public void CreateCollection(IClientSessionHandle session, string name, CreateCollectionOptions options = null, CancellationToken cancellationToken = default)
        {
            CreateCollection(name);
        }

        public Task CreateCollectionAsync(string name, CreateCollectionOptions options = null, CancellationToken cancellationToken = default)
        {
            CreateCollection(name);
            return Task.CompletedTask;
        }

        public Task CreateCollectionAsync(IClientSessionHandle session, string name, CreateCollectionOptions options = null, CancellationToken cancellationToken = default)
        {
            CreateCollection(name);
            return Task.CompletedTask;
        }

        public void CreateView<TDocument, TResult>(string viewName, string viewOn, PipelineDefinition<TDocument, TResult> pipeline, CreateViewOptions<TDocument> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public void CreateView<TDocument, TResult>(IClientSessionHandle session, string viewName, string viewOn, PipelineDefinition<TDocument, TResult> pipeline, CreateViewOptions<TDocument> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task CreateViewAsync<TDocument, TResult>(string viewName, string viewOn, PipelineDefinition<TDocument, TResult> pipeline, CreateViewOptions<TDocument> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task CreateViewAsync<TDocument, TResult>(IClientSessionHandle session, string viewName, string viewOn, PipelineDefinition<TDocument, TResult> pipeline, CreateViewOptions<TDocument> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public void DropCollection(string name, CancellationToken cancellationToken = default)
        {
            collections.TryRemove(name, out var col);
        }

        public void DropCollection(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
        {
            DropCollection(name);
        }

        public Task DropCollectionAsync(string name, CancellationToken cancellationToken = default)
        {
            DropCollection(name);
            return Task.CompletedTask;
        }

        public Task DropCollectionAsync(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
        {
            DropCollection(name);
            return Task.CompletedTask;
        }

        public IMongoCollection<TDocument> GetCollection<TDocument>(string name, MongoCollectionSettings settings = null)
        {
            InMemoryCollection<TDocument> NewCollection(VanillaCollection vCol)
            {
                return new InMemoryCollection<TDocument>(this, vCol, iocContainer.Resolve<IFilter>(), iocContainer.Resolve<IUpdater>());
            }

            return NewCollection(collections.GetOrAdd(name, n => new VanillaCollection(n)));// as IMongoCollection<TDocument>;//, (n, c) => c ?? NewCollection(n)) as IMongoCollection<TDocument>;
        }

        public IAsyncCursor<BsonDocument> ListCollections(ListCollectionsOptions options = null, CancellationToken cancellationToken = default)
        {
            return INMAsyncCursor.Create(collections.Keys.Select(n => (new { name = n }).ToBsonDocument()));
        }

        public IAsyncCursor<BsonDocument> ListCollections(IClientSessionHandle session, ListCollectionsOptions options = null, CancellationToken cancellationToken = default)
        {
            return ListCollections();
        }

        public Task<IAsyncCursor<BsonDocument>> ListCollectionsAsync(ListCollectionsOptions options = null, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(ListCollections());
        }

        public Task<IAsyncCursor<BsonDocument>> ListCollectionsAsync(IClientSessionHandle session, ListCollectionsOptions options = null, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(ListCollections());
        }

        public void RenameCollection(string oldName, string newName, RenameCollectionOptions options = null, CancellationToken cancellationToken = default)
        {
            if (collections.TryGetValue(oldName, out var col))
                if (collections.TryAdd(newName, col))
                    return;
                else
                    throw new InMemoryDatabaseException($"A collection already exists named '{newName}'");
            else
                throw new InMemoryDatabaseException($"Could not find collection '{oldName}'");

        }

        public void RenameCollection(IClientSessionHandle session, string oldName, string newName, RenameCollectionOptions options = null, CancellationToken cancellationToken = default)
        {
            RenameCollection(oldName, newName);
        }

        public Task RenameCollectionAsync(string oldName, string newName, RenameCollectionOptions options = null, CancellationToken cancellationToken = default)
        {
            RenameCollection(oldName, newName);
            return Task.CompletedTask;
        }

        public Task RenameCollectionAsync(IClientSessionHandle session, string oldName, string newName, RenameCollectionOptions options = null, CancellationToken cancellationToken = default)
        {
            RenameCollection(oldName, newName);
            return Task.CompletedTask;
        }

        public TResult RunCommand<TResult>(Command<TResult> command, ReadPreference readPreference = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public TResult RunCommand<TResult>(IClientSessionHandle session, Command<TResult> command, ReadPreference readPreference = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<TResult> RunCommandAsync<TResult>(Command<TResult> command, ReadPreference readPreference = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<TResult> RunCommandAsync<TResult>(IClientSessionHandle session, Command<TResult> command, ReadPreference readPreference = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IMongoDatabase WithReadConcern(ReadConcern readConcern) => this;

        public IMongoDatabase WithReadPreference(ReadPreference readPreference) => this;

        public IMongoDatabase WithWriteConcern(WriteConcern writeConcern) => this;
    }


    [Serializable]
    public class InMemoryDatabaseException : Exception
    {
        public InMemoryDatabaseException() { }
        public InMemoryDatabaseException(string message) : base(message) { }
        public InMemoryDatabaseException(string message, Exception inner) : base(message, inner) { }
        protected InMemoryDatabaseException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
