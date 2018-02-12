using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
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
    internal class Collection_Installer : ITinyIoCInstaller
    {
        public void Install(TinyIoCContainer container)
        {
            container.Register(typeof(InMemoryCollection<>));
        }
    }
    class InMemoryCollection
    {

    }
    class InMemoryCollection<T> : InMemoryCollection, IMongoCollection<T>
    {
        private readonly ConcurrentDictionary<object, BsonDocument> docs = new ConcurrentDictionary<object, BsonDocument>();
        private readonly string name;
        private readonly IFilter whereFilter;
        private readonly IIdGenerator idGenerator;
        private readonly BsonMemberMap idMemeber;
        private readonly IBsonSerializer<T> bsonSerializer;
        public InMemoryCollection(IMongoDatabase db, string name, IFilter whereFilter)
        {
            Database = db ?? throw new ArgumentNullException(nameof(db));
            this.name = name;
            this.whereFilter = whereFilter ?? throw new ArgumentNullException(nameof(whereFilter));

            var map = MongoDB.Bson.Serialization.BsonClassMap.LookupClassMap(typeof(T));
            idGenerator = (idMemeber = map.IdMemberMap).IdGenerator;
            bsonSerializer = BsonSerializer.SerializerRegistry.GetSerializer<T>();
        }

        public CollectionNamespace CollectionNamespace => new CollectionNamespace(Database.DatabaseNamespace, name);

        public IMongoDatabase Database { get; private set; }

        public IBsonSerializer<T> DocumentSerializer => BsonSerializer.SerializerRegistry.GetSerializer<T>();

        public IMongoIndexManager<T> Indexes => throw new NotImplementedException();

        public MongoCollectionSettings Settings => new MongoCollectionSettings();
        private IEnumerable<BsonDocument> AllDocs()
            => docs.Select(i => i.Value);

        public IAsyncCursor<TResult> Aggregate<TResult>(PipelineDefinition<T, TResult> pipeline, AggregateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IAsyncCursor<TResult> Aggregate<TResult>(IClientSessionHandle session, PipelineDefinition<T, TResult> pipeline, AggregateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncCursor<TResult>> AggregateAsync<TResult>(PipelineDefinition<T, TResult> pipeline, AggregateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncCursor<TResult>> AggregateAsync<TResult>(IClientSessionHandle session, PipelineDefinition<T, TResult> pipeline, AggregateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public BulkWriteResult<T> BulkWrite(IEnumerable<WriteModel<T>> requests, BulkWriteOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public BulkWriteResult<T> BulkWrite(IClientSessionHandle session, IEnumerable<WriteModel<T>> requests, BulkWriteOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<BulkWriteResult<T>> BulkWriteAsync(IEnumerable<WriteModel<T>> requests, BulkWriteOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<BulkWriteResult<T>> BulkWriteAsync(IClientSessionHandle session, IEnumerable<WriteModel<T>> requests, BulkWriteOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        private IEnumerable<BsonDocument> ApplyFilter(FilterDefinition<T> filter)
            => whereFilter.Apply(filter, AllDocs());

        public long Count(FilterDefinition<T> filter, CountOptions options = null, CancellationToken cancellationToken = default)
            => ApplyFilter(filter).Count();

        public long Count(IClientSessionHandle session, FilterDefinition<T> filter, CountOptions options = null, CancellationToken cancellationToken = default)
            => Count(filter, options, cancellationToken);

        public Task<long> CountAsync(FilterDefinition<T> filter, CountOptions options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(Count(filter, options, cancellationToken));

        public Task<long> CountAsync(IClientSessionHandle session, FilterDefinition<T> filter, CountOptions options = null, CancellationToken cancellationToken = default)
            => CountAsync(filter, options, cancellationToken);

        public DeleteResult DeleteMany(FilterDefinition<T> filter, CancellationToken cancellationToken = default)
        {
            int count = 0;
            foreach (var doc in ApplyFilter(filter))
                if (RemoveDoc(doc))
                    count++;
            return new INMDeleteResult(count, true);
        }

        private bool RemoveDoc(BsonDocument doc)
            => doc != null && docs.TryRemove(doc["_id"], out var _);

        public DeleteResult DeleteMany(FilterDefinition<T> filter, DeleteOptions options, CancellationToken cancellationToken = default)
            => DeleteMany(filter, cancellationToken);

        public DeleteResult DeleteMany(IClientSessionHandle session, FilterDefinition<T> filter, DeleteOptions options = null, CancellationToken cancellationToken = default)
            => DeleteMany(filter, cancellationToken);

        public Task<DeleteResult> DeleteManyAsync(FilterDefinition<T> filter, CancellationToken cancellationToken = default)
            => Task.FromResult(DeleteMany(filter, cancellationToken));

        public Task<DeleteResult> DeleteManyAsync(FilterDefinition<T> filter, DeleteOptions options, CancellationToken cancellationToken = default)
            => DeleteManyAsync(filter, cancellationToken);

        public Task<DeleteResult> DeleteManyAsync(IClientSessionHandle session, FilterDefinition<T> filter, DeleteOptions options = null, CancellationToken cancellationToken = default)
            => DeleteManyAsync(filter, cancellationToken);

        public DeleteResult DeleteOne(FilterDefinition<T> filter, CancellationToken cancellationToken = default)
            => new INMDeleteResult(RemoveDoc(ApplyFilter(filter).FirstOrDefault()) ? 1 : 0, true);

        public DeleteResult DeleteOne(FilterDefinition<T> filter, DeleteOptions options, CancellationToken cancellationToken = default)
            => DeleteOne(filter, cancellationToken);

        public DeleteResult DeleteOne(IClientSessionHandle session, FilterDefinition<T> filter, DeleteOptions options = null, CancellationToken cancellationToken = default)
            => DeleteOne(filter, cancellationToken);

        public Task<DeleteResult> DeleteOneAsync(FilterDefinition<T> filter, CancellationToken cancellationToken = default)
            => Task.FromResult(DeleteOne(filter, cancellationToken));

        public Task<DeleteResult> DeleteOneAsync(FilterDefinition<T> filter, DeleteOptions options, CancellationToken cancellationToken = default)
            => Task.FromResult(DeleteOne(filter, cancellationToken));

        public Task<DeleteResult> DeleteOneAsync(IClientSessionHandle session, FilterDefinition<T> filter, DeleteOptions options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(DeleteOne(filter, cancellationToken));

        private (string Name, Func<BsonValue, TField> Deserializer) GetFieldName<TField>(FieldDefinition<T, TField> fieldDefinition)
        {
            var ser = MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry.GetSerializer<T>();
            var doc = fieldDefinition.Render(ser, MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry);

            return (doc.FieldName, v =>
            {
                using (var bsonReader = new JsonReader(v.ToJson()))
                {
                    return doc.FieldSerializer.Deserialize<TField>(BsonDeserializationContext.CreateRoot(bsonReader));
                }
            }
            );
        }
        public IAsyncCursor<TField> Distinct<TField>(FieldDefinition<T, TField> field, FilterDefinition<T> filter, DistinctOptions options = null, CancellationToken cancellationToken = default)
        {
            var fld = GetFieldName(field);
            return INMAsyncCursor.Create(DistinctImpl(ApplyFilter(filter), fld));
        }

        private IEnumerable<TField> DistinctImpl<TField>(IEnumerable<BsonDocument> items, (string Name, Func<BsonValue, TField> Deserializer) fld)
            => items.Select(i => fld.Deserializer(BsonHelpers.GetDocumentValue(fld.Name, i))).Distinct();

        public IAsyncCursor<TField> Distinct<TField>(IClientSessionHandle session, FieldDefinition<T, TField> field, FilterDefinition<T> filter, DistinctOptions options = null, CancellationToken cancellationToken = default)
            => Distinct(field, filter, options, cancellationToken);

        public Task<IAsyncCursor<TField>> DistinctAsync<TField>(FieldDefinition<T, TField> field, FilterDefinition<T> filter, DistinctOptions options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(Distinct(field, filter, options, cancellationToken));

        public Task<IAsyncCursor<TField>> DistinctAsync<TField>(IClientSessionHandle session, FieldDefinition<T, TField> field, FilterDefinition<T> filter, DistinctOptions options = null, CancellationToken cancellationToken = default)
            => DistinctAsync(field, filter, options, cancellationToken);

        public Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(FilterDefinition<T> filter, FindOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            var projected = FilterAndProject(filter, options.Projection);

            return Task.FromResult(INMAsyncCursor.Create(projected));
        }

        private IEnumerable<TProjection> FilterAndProject<TProjection>(FilterDefinition<T> filter, ProjectionDefinition<T, TProjection> projection)
        {
            var actualProjection = projection ?? new ClientSideDeserializationProjectionDefinition<T, TProjection>();
            var renderedProjection = actualProjection.Render(BsonSerializer.SerializerRegistry.GetSerializer<T>(), BsonSerializer.SerializerRegistry);

            var items = ApplyFilter(filter);

            var projected = items.Select(doc => DeserialzeProjection(renderedProjection.ProjectionSerializer, doc));

            return projected;
        }
        private TProjection ProjectDocument<TProjection>(ProjectionDefinition<T, TProjection> projection, BsonDocument doc)
        {
            var actualProjection = projection ?? new ClientSideDeserializationProjectionDefinition<T, TProjection>();
            var renderedProjection = actualProjection.Render(BsonSerializer.SerializerRegistry.GetSerializer<T>(), BsonSerializer.SerializerRegistry);
            return DeserialzeProjection(renderedProjection.ProjectionSerializer, doc);
        }

        private TProjection DeserialzeProjection<TProjection>(IBsonSerializer<TProjection> preojectionSerializer, BsonDocument doc)
        {
            using (var stream = new System.IO.MemoryStream())
            {
                using (var writer = new BsonBinaryWriter(stream))
                {
                    var writeCtx = BsonSerializationContext.CreateRoot(writer);

                    bsonSerializer.Serialize(writeCtx, doc);
                }

                stream.Seek(0, System.IO.SeekOrigin.Begin);
                using (var r = new BsonBinaryReader(stream))
                {
                    var reader = new BsonBinaryReader(stream);
                    var readCtx = BsonDeserializationContext.CreateRoot(reader);
                    return preojectionSerializer.Deserialize(readCtx);
                }
            }
        }

        public Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, FindOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => FindAsync(filter, options, cancellationToken);

        public TProjection FindOneAndDelete<TProjection>(FilterDefinition<T> filter, FindOneAndDeleteOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            var filteredDocs = ApplyFilter(filter);
            var one = filteredDocs.FirstOrDefault();
            if (one == null)
                return default;
            docs.TryRemove(one["_id"], out var _);
            return ProjectDocument(options.Projection, one);
        }

        public TProjection FindOneAndDelete<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, FindOneAndDeleteOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<TProjection> FindOneAndDeleteAsync<TProjection>(FilterDefinition<T> filter, FindOneAndDeleteOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<TProjection> FindOneAndDeleteAsync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, FindOneAndDeleteOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public TProjection FindOneAndReplace<TProjection>(FilterDefinition<T> filter, T replacement, FindOneAndReplaceOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public TProjection FindOneAndReplace<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, T replacement, FindOneAndReplaceOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<TProjection> FindOneAndReplaceAsync<TProjection>(FilterDefinition<T> filter, T replacement, FindOneAndReplaceOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<TProjection> FindOneAndReplaceAsync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, T replacement, FindOneAndReplaceOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public TProjection FindOneAndUpdate<TProjection>(FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public TProjection FindOneAndUpdate<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<TProjection> FindOneAndUpdateAsync<TProjection>(FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<TProjection> FindOneAndUpdateAsync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IAsyncCursor<TProjection> FindSync<TProjection>(FilterDefinition<T> filter, FindOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IAsyncCursor<TProjection> FindSync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, FindOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public void InsertMany(IEnumerable<T> documents, InsertManyOptions options = null, CancellationToken cancellationToken = default)
        {
            var oneOptions = options == null ? null : new InsertOneOptions { BypassDocumentValidation = options.BypassDocumentValidation };
            foreach (var doc in documents)
                InsertOne(doc, oneOptions, cancellationToken);
        }

        public void InsertMany(IClientSessionHandle session, IEnumerable<T> documents, InsertManyOptions options = null, CancellationToken cancellationToken = default)
        {
            InsertMany(documents, options, cancellationToken);
        }

        public Task InsertManyAsync(IEnumerable<T> documents, InsertManyOptions options = null, CancellationToken cancellationToken = default)
        {
            InsertMany(documents, options, cancellationToken);
            return Task.CompletedTask;
        }

        public Task InsertManyAsync(IClientSessionHandle session, IEnumerable<T> documents, InsertManyOptions options = null, CancellationToken cancellationToken = default)
        {
            InsertMany(documents, options, cancellationToken);
            return Task.CompletedTask;
        }

        public void InsertOne(T document, InsertOneOptions options = null, CancellationToken cancellationToken = default)
        {
            object docId = idMemeber.Getter(document);
            if (docId == null || docId.Equals(idMemeber.DefaultValue))
                idMemeber.Setter(document, idGenerator.GenerateId(this, document));
            var bson = document.ToBsonDocument();
            docs.TryAdd(bson["_id"], bson);
        }

        public void InsertOne(IClientSessionHandle session, T document, InsertOneOptions options = null, CancellationToken cancellationToken = default)
        {
            InsertOne(document, options, cancellationToken);
        }

        public Task InsertOneAsync(T document, InsertOneOptions options = null, CancellationToken cancellationToken = default)
        {
            InsertOne(document, options, cancellationToken);
            return Task.CompletedTask;
        }

        public Task InsertOneAsync(T document, CancellationToken cancellationToken = default)
        {
            InsertOne(document, null, cancellationToken);
            return Task.CompletedTask;
        }

        public Task InsertOneAsync(IClientSessionHandle session, T document, InsertOneOptions options = null, CancellationToken cancellationToken = default)
        {
            InsertOne(document, options, cancellationToken);
            return Task.CompletedTask;
        }

        public IAsyncCursor<TResult> MapReduce<TResult>(BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<T, TResult> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IAsyncCursor<TResult> MapReduce<TResult>(IClientSessionHandle session, BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<T, TResult> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncCursor<TResult>> MapReduceAsync<TResult>(BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<T, TResult> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncCursor<TResult>> MapReduceAsync<TResult>(IClientSessionHandle session, BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<T, TResult> options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IFilteredMongoCollection<TDerivedDocument> OfType<TDerivedDocument>() where TDerivedDocument : T
        {
            throw new NotImplementedException();
        }

        public ReplaceOneResult ReplaceOne(FilterDefinition<T> filter, T replacement, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public ReplaceOneResult ReplaceOne(IClientSessionHandle session, FilterDefinition<T> filter, T replacement, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<ReplaceOneResult> ReplaceOneAsync(FilterDefinition<T> filter, T replacement, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<ReplaceOneResult> ReplaceOneAsync(IClientSessionHandle session, FilterDefinition<T> filter, T replacement, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public UpdateResult UpdateMany(FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public UpdateResult UpdateMany(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<UpdateResult> UpdateManyAsync(FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<UpdateResult> UpdateManyAsync(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public UpdateResult UpdateOne(FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public UpdateResult UpdateOne(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<UpdateResult> UpdateOneAsync(FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<UpdateResult> UpdateOneAsync(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IAsyncCursor<TResult> Watch<TResult>(PipelineDefinition<ChangeStreamDocument<T>, TResult> pipeline, ChangeStreamOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IAsyncCursor<TResult> Watch<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<T>, TResult> pipeline, ChangeStreamOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncCursor<TResult>> WatchAsync<TResult>(PipelineDefinition<ChangeStreamDocument<T>, TResult> pipeline, ChangeStreamOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncCursor<TResult>> WatchAsync<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<T>, TResult> pipeline, ChangeStreamOptions options = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public IMongoCollection<T> WithReadConcern(ReadConcern readConcern)
        {
            throw new NotImplementedException();
        }

        public IMongoCollection<T> WithReadPreference(ReadPreference readPreference)
        {
            throw new NotImplementedException();
        }

        public IMongoCollection<T> WithWriteConcern(WriteConcern writeConcern)
        {
            throw new NotImplementedException();
        }
    }
}
