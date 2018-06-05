using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
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

    class InMemoryCollection<T> : IMongoCollection<T>
    {
        //private readonly ConcurrentDictionary<object, BsonDocument> Docs;
        private readonly VanillaCollection vCol;
        //private readonly string Name;
        private readonly IFilter whereFilter;
        private readonly IUpdater updater;
        private readonly IIdGenerator idGenerator;
        private readonly IBsonSerializer<T> bsonSerializer;
        private readonly BsonMemberMap idMemeber;

        private string Name => vCol.Name;
        private ConcurrentDictionary<object, BsonDocument> Docs => vCol.Docs;
        public InMemoryCollection(IMongoDatabase db, VanillaCollection vCol, IFilter whereFilter, IUpdater updater)
        {
            Database = db ?? throw new ArgumentNullException(nameof(db));
            this.vCol = vCol ?? throw new ArgumentNullException(nameof(vCol));
            this.whereFilter = whereFilter ?? throw new ArgumentNullException(nameof(whereFilter));
            this.updater = updater ?? throw new ArgumentNullException(nameof(updater));
            var map = BsonClassMap.LookupClassMap(typeof(T));

            idGenerator = (idMemeber = map.IdMemberMap)?.IdGenerator;
            bsonSerializer = BsonSerializer.SerializerRegistry.GetSerializer<T>();
        }

        public CollectionNamespace CollectionNamespace => new CollectionNamespace(Database.DatabaseNamespace, Name);

        public IMongoDatabase Database { get; private set; }

        public IBsonSerializer<T> DocumentSerializer => BsonSerializer.SerializerRegistry.GetSerializer<T>();

        public IMongoIndexManager<T> Indexes => throw new NotImplementedException();

        public MongoCollectionSettings Settings => new MongoCollectionSettings();
        private IEnumerable<BsonDocument> AllDocs()
            => Docs.Select(i => i.Value);

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
            var bypassDocumentValidation = options?.BypassDocumentValidation;
            long deleted = 0;
            long inserted = 0;
            long matched = 0;
            long modified = 0;
            int count = 0;
            var upserts = new List<BulkWriteUpsert>();
            foreach (var req in requests)
            {
                switch (req)
                {
                    case InsertOneModel<T> insert:
                        InsertOne(insert.Document);
                        inserted++;
                        break;
                    case ReplaceOneModel<T> replace:
                        var rOneResult = ReplaceOne(replace.Filter, replace.Replacement, new UpdateOptions { IsUpsert = replace.IsUpsert, BypassDocumentValidation = bypassDocumentValidation });
                        matched += rOneResult.MatchedCount;
                        if (rOneResult.MatchedCount == 0)
                        {
                            upserts.Add(INMBulkWriteUpsert.Create(count, rOneResult.UpsertedId));
                            inserted++;
                        }
                        break;
                    case UpdateOneModel<T> update:
                        var uOneResult = UpdateOne(update.Filter, update.Update, new UpdateOptions { ArrayFilters = update.ArrayFilters, IsUpsert = update.IsUpsert, BypassDocumentValidation = bypassDocumentValidation });
                        matched += uOneResult.MatchedCount;
                        modified += uOneResult.ModifiedCount;
                        if (uOneResult.ModifiedCount != uOneResult.MatchedCount)
                            upserts.Add(INMBulkWriteUpsert.Create(count, uOneResult.UpsertedId));
                        break;
                    case UpdateManyModel<T> updateMany:
                        var uManuResult = UpdateMany(updateMany.Filter, updateMany.Update, new UpdateOptions { ArrayFilters = updateMany.ArrayFilters, IsUpsert = updateMany.IsUpsert, BypassDocumentValidation = bypassDocumentValidation });
                        matched += uManuResult.MatchedCount;
                        modified += uManuResult.ModifiedCount;
                        if (uManuResult.ModifiedCount != uManuResult.MatchedCount)
                            upserts.Add(INMBulkWriteUpsert.Create(count, uManuResult.UpsertedId));
                        break;
                    case DeleteOneModel<T> delete:
                        var dOneResult = DeleteOne(delete.Filter);
                        deleted += dOneResult.DeletedCount;
                        break;
                    case DeleteManyModel<T> deleteMany:
                        var dManyResult = DeleteMany(deleteMany.Filter);
                        deleted += dManyResult.DeletedCount;
                        break;
                    default:
                        throw new InMemoryDatabaseException($"Unknown WriteModel type '{req.GetType().Name}'");
                }
                count++;
            }
            return new INMBulkWriteResult<T>(count, requests)
            {
                INMDeletedCount = deleted,
                IMNUpserts = upserts,
                INMInsertedCount = inserted,
                INMMatchedCount = matched,
                INMModifiedCount = modified
            };
        }

        public BulkWriteResult<T> BulkWrite(IClientSessionHandle session, IEnumerable<WriteModel<T>> requests, BulkWriteOptions options = null, CancellationToken cancellationToken = default)
            => BulkWrite(requests, options, cancellationToken);

        public Task<BulkWriteResult<T>> BulkWriteAsync(IEnumerable<WriteModel<T>> requests, BulkWriteOptions options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(BulkWrite(requests, options, cancellationToken));

        public Task<BulkWriteResult<T>> BulkWriteAsync(IClientSessionHandle session, IEnumerable<WriteModel<T>> requests, BulkWriteOptions options = null, CancellationToken cancellationToken = default)
            => BulkWriteAsync(requests, options, cancellationToken);

        protected virtual IEnumerable<BsonDocument> ApplyFilter(FilterDefinition<T> filter, long? limit, long? skip)
            => whereFilter.Apply(filter, AllDocs(), limit, skip);

        public long Count(FilterDefinition<T> filter, CountOptions options = null, CancellationToken cancellationToken = default)
            => ApplyFilter(filter, options?.Limit, options?.Skip).Count();

        public long Count(IClientSessionHandle session, FilterDefinition<T> filter, CountOptions options = null, CancellationToken cancellationToken = default)
            => Count(filter, options, cancellationToken);

        public Task<long> CountAsync(FilterDefinition<T> filter, CountOptions options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(Count(filter, options, cancellationToken));

        public Task<long> CountAsync(IClientSessionHandle session, FilterDefinition<T> filter, CountOptions options = null, CancellationToken cancellationToken = default)
            => CountAsync(filter, options, cancellationToken);

        public DeleteResult DeleteMany(FilterDefinition<T> filter, CancellationToken cancellationToken = default)
        {
            int count = 0;
            foreach (var doc in ApplyFilter(filter, null, null))
                if (RemoveDoc(doc))
                    count++;
            return new INMDeleteResult(count, true);
        }

        private bool RemoveDoc(BsonDocument doc)
            => doc != null && Docs.TryRemove(doc["_id"], out var _);

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
            => new INMDeleteResult(RemoveDoc(ApplyFilter(filter, null, null).FirstOrDefault()) ? 1 : 0, true);

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
            var doc = fieldDefinition.Render(bsonSerializer, BsonSerializer.SerializerRegistry);

            return (doc.FieldName, v =>
            {
                using (var bsonReader = new JsonReader(v.ToJson()))
                {
                    return doc.FieldSerializer.Deserialize<TField>(BsonDeserializationContext.CreateRoot(bsonReader));
                }
            });
        }
        public IAsyncCursor<TField> Distinct<TField>(FieldDefinition<T, TField> field, FilterDefinition<T> filter, DistinctOptions options = null, CancellationToken cancellationToken = default)
        {
            var fld = GetFieldName(field);
            return INMAsyncCursor.Create(DistinctImpl(ApplyFilter(filter, null, null), fld));
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
            => Task.FromResult(FindSync(filter, options, cancellationToken));

        private IEnumerable<TProjection> FilterAndProject<TProjection>(FilterDefinition<T> filter, ProjectionDefinition<T, TProjection> projection, long? limit, long? skip, SortDefinition<T> sort)
        {
            var proj = projection ?? new ClientSideDeserializationProjectionDefinition<T, TProjection>();
            var projSerializer = (proj.Render(bsonSerializer, BsonSerializer.SerializerRegistry)).ProjectionSerializer;
            return ApplySort(sort, ApplyFilter(filter, limit, skip)).Select(item => DoProjection(item, projSerializer));
        }

        public TProjection FindOneAndDelete<TProjection>(FilterDefinition<T> filter, FindOneAndDeleteOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            var item = ApplySort(options?.Sort, ApplyFilter(filter, null, null)).First();
            if (item != null)
            {
                Docs.TryRemove(item["_id"], out var _);
                return DoProjection(item, options?.Projection);
            }
            return default;
        }

        private IEnumerable<BsonDocument> ApplySort(SortDefinition<T> sort, IEnumerable<BsonDocument> items)
        {
            if (sort == null)
                return items;

            var sortKeys = sort.Render(bsonSerializer, BsonSerializer.SerializerRegistry);
            foreach(var key in sortKeys)
            {
                if (key.Value == 1)
                    items = items.OrderBy(doc => BsonHelpers.GetDocumentValue(key.Name, doc));
                else
                    items = items.OrderByDescending(doc => BsonHelpers.GetDocumentValue(key.Name, doc));
            }
            return items;
        }

        private TProjection DoProjection<TProjection>(BsonDocument item, ProjectionDefinition<T, TProjection> projection)
        {
            if (item == null)
                return default;
            var proj = projection ?? new ClientSideDeserializationProjectionDefinition<T, TProjection>();
            var projSerializer = (proj.Render(bsonSerializer, BsonSerializer.SerializerRegistry)).ProjectionSerializer;
            return DoProjection(item, projSerializer);
        }

        private TProjection DoProjection<TProjection>(BsonDocument item, IBsonSerializer<TProjection> deserializer)
        {
            using (var stream = new MemoryStream())
            {
                var documentSerializer = BsonSerializer.SerializerRegistry.GetSerializer<BsonDocument>();
                using (var writer = new BsonBinaryWriter(stream))
                    documentSerializer.Serialize(BsonSerializationContext.CreateRoot(writer), item);
                stream.Seek(0, SeekOrigin.Begin);
                using (var reader = new BsonBinaryReader(stream))
                    return deserializer.Deserialize(BsonDeserializationContext.CreateRoot(reader));
            }
        }

        public TProjection FindOneAndDelete<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, FindOneAndDeleteOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => FindOneAndDelete(filter, options, cancellationToken);

        public Task<TProjection> FindOneAndDeleteAsync<TProjection>(FilterDefinition<T> filter, FindOneAndDeleteOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(FindOneAndDelete(filter, options, cancellationToken));

        public Task<TProjection> FindOneAndDeleteAsync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, FindOneAndDeleteOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(FindOneAndDelete(filter, options, cancellationToken));

        public TProjection FindOneAndReplace<TProjection>(FilterDefinition<T> filter, T replacement, FindOneAndReplaceOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            var item = ApplyFilter(filter, null, null).FirstOrDefault();
            if (item != null)
            {
                var id = idMemeber.Getter(item);

                var newId = idMemeber.Getter(replacement);
                if (newId != null && !newId.Equals(idMemeber.DefaultValue) && !id.Equals(newId))
                    throw new InMemoryDatabaseException($"The _id field cannot be changed from {{{id}}} to {{{newId}}}.");

                var bson = replacement.ToBsonDocument();
                Docs[bson["_id"]] = bson;
            }
            else if(options != null && options.IsUpsert)
                InsertOne(replacement);
            return DoProjection(item, options?.Projection);
        }

        public TProjection FindOneAndReplace<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, T replacement, FindOneAndReplaceOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => FindOneAndReplace(filter, replacement, options, cancellationToken);

        public Task<TProjection> FindOneAndReplaceAsync<TProjection>(FilterDefinition<T> filter, T replacement, FindOneAndReplaceOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(FindOneAndReplace(filter, replacement, options, cancellationToken));

        public Task<TProjection> FindOneAndReplaceAsync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, T replacement, FindOneAndReplaceOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(FindOneAndReplace(filter, replacement, options, cancellationToken));

        public TProjection FindOneAndUpdate<TProjection>(FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            // When upserting we are ignoring the values we should take from the filter
            var currentItem = ApplySort(options?.Sort, ApplyFilter(filter, null, null)).First();
            BsonDocument docToReturn = currentItem;
            if (currentItem == null && options != null && options.IsUpsert)
            {
                currentItem = new BsonDocument();
                if(idGenerator != null)
                    currentItem["_id"] = BsonValue.Create(idGenerator.GenerateId(this, currentItem));
            }
            if (currentItem == null)
                return default;

            if ((options?.ReturnDocument ?? ReturnDocument.Before) == ReturnDocument.Before)
                docToReturn = docToReturn?.DeepClone()?.AsBsonDocument;
            else
                docToReturn = currentItem;

            updater.Apply(update, currentItem);

            return DoProjection(docToReturn, options.Projection);
        }

        public TProjection FindOneAndUpdate<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => FindOneAndUpdate(filter, update, options, cancellationToken);

        public Task<TProjection> FindOneAndUpdateAsync<TProjection>(FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(FindOneAndUpdate(filter, update, options, cancellationToken));

        public Task<TProjection> FindOneAndUpdateAsync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => FindOneAndUpdateAsync(filter, update, options, cancellationToken);

        public IAsyncCursor<TProjection> FindSync<TProjection>(FilterDefinition<T> filter, FindOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
        {
            var items = FilterAndProject(filter, options?.Projection, options?.Limit, options?.Skip, options?.Sort);
            return INMAsyncCursor.Create(items);
        }

        public IAsyncCursor<TProjection> FindSync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, FindOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => FindSync(filter, options, cancellationToken);

        public void InsertMany(IEnumerable<T> documents, InsertManyOptions options = null, CancellationToken cancellationToken = default)
        {
            var oneOptions = options == null ? null : new InsertOneOptions { BypassDocumentValidation = options.BypassDocumentValidation };
            foreach (var doc in documents)
                InsertOne(doc, oneOptions, cancellationToken);
        }

        public void InsertMany(IClientSessionHandle session, IEnumerable<T> documents, InsertManyOptions options = null, CancellationToken cancellationToken = default)
            => InsertMany(documents, options, cancellationToken);

        public Task InsertManyAsync(IEnumerable<T> documents, InsertManyOptions options = null, CancellationToken cancellationToken = default)
        {
            InsertMany(documents, options, cancellationToken);
            return Task.CompletedTask;
        }

        public Task InsertManyAsync(IClientSessionHandle session, IEnumerable<T> documents, InsertManyOptions options = null, CancellationToken cancellationToken = default)
            => InsertManyAsync(documents, options, cancellationToken);

        public void InsertOne(T document, InsertOneOptions options = null, CancellationToken cancellationToken = default)
        {
            object docId = idMemeber.Getter(document);
            if (docId == null || (idMemeber != null && docId.Equals(idMemeber.DefaultValue)))
                idMemeber.Setter(document, idGenerator.GenerateId(this, document));
            var bson = document.ToBsonDocument();
            Docs.TryAdd(bson["_id"], bson);
        }

        public void InsertOne(IClientSessionHandle session, T document, InsertOneOptions options = null, CancellationToken cancellationToken = default)
            => InsertOne(document, options, cancellationToken);

        public Task InsertOneAsync(T document, InsertOneOptions options = null, CancellationToken cancellationToken = default)
        {
            InsertOne(document, options, cancellationToken);
            return Task.CompletedTask;
        }

        public Task InsertOneAsync(T document, CancellationToken cancellationToken = default)
            => InsertOneAsync(document, null, cancellationToken);

        public Task InsertOneAsync(IClientSessionHandle session, T document, InsertOneOptions options = null, CancellationToken cancellationToken = default)
            => InsertOneAsync(document, null, cancellationToken);

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
            => new InMemoryFilteredCollection<T, TDerivedDocument>(Database, vCol, whereFilter, updater, Builders<TDerivedDocument>.Filter.OfType<TDerivedDocument>(_ => true));

        public ReplaceOneResult ReplaceOne(FilterDefinition<T> filter, T replacement, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            var item = ApplyFilter(filter, null, null).FirstOrDefault();
            if (item != null)
            {
                var id = idMemeber.Getter(item);

                var newId = idMemeber.Getter(replacement);
                if (newId != null && !newId.Equals(idMemeber.DefaultValue) && !id.Equals(newId))
                    throw new InMemoryDatabaseException($@"The _id field cannot be changed from {{{id}}} to {{{newId}}}.");

                idMemeber.Setter(replacement, id);
                var bson = replacement.ToBsonDocument();
                Docs[bson["_id"]] = bson;
                return new IMReplaceOneResult(true, true, 1, 1, BsonValue.Create(id));
            }
            else if (options != null && options.IsUpsert)
            {
                InsertOne(replacement);
                return new IMReplaceOneResult(true, true, 0, 1, BsonValue.Create(idMemeber.Getter(replacement)));
            }

            return new IMReplaceOneResult(true, true, 0, 0, BsonValue.Create(null));
        }

        public ReplaceOneResult ReplaceOne(IClientSessionHandle session, FilterDefinition<T> filter, T replacement, UpdateOptions options = null, CancellationToken cancellationToken = default)
            => ReplaceOne(filter, replacement, options, cancellationToken);

        public Task<ReplaceOneResult> ReplaceOneAsync(FilterDefinition<T> filter, T replacement, UpdateOptions options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(ReplaceOne(filter, replacement, options, cancellationToken));

        public Task<ReplaceOneResult> ReplaceOneAsync(IClientSessionHandle session, FilterDefinition<T> filter, T replacement, UpdateOptions options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(ReplaceOne(filter, replacement, options, cancellationToken));

        public UpdateResult UpdateMany(FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            // When upserting we are ignoring the values we should take from the filter
            var items = ApplyFilter(filter, null, null);
            var updates = updater.Apply(update, items);
            BsonValue upsetId = BsonNull.Value;

            if (options != null && options.IsUpsert && updates == 0)
            {
                upsetId = DoUpsert(update);
            }
            return new INMUpdateResult(true, updates, updates, upsetId);
        }

        private BsonValue DoUpsert(UpdateDefinition<T> update)
        {
            BsonValue upsetId = null;
            var doc = new BsonDocument();
            if(idGenerator != null)
                doc["_id"] = upsetId = BsonValue.Create(idGenerator.GenerateId(this, doc));
            updater.Apply(update, doc);
            if (idGenerator != null)
                Docs[doc["_id"]] = doc;
            return upsetId;
        }

        public UpdateResult UpdateMany(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
            => UpdateMany(filter, update, options, cancellationToken);

        public Task<UpdateResult> UpdateManyAsync(FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(UpdateMany(filter, update, options, cancellationToken));

        public Task<UpdateResult> UpdateManyAsync(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
            => UpdateManyAsync(filter, update, options, cancellationToken);

        public UpdateResult UpdateOne(FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
        {
            var item = ApplyFilter(filter, null, null).FirstOrDefault();
            BsonValue upsertId = BsonNull.Value;
            int matched = 0;
            int modified = 0;
            if (item != null)
            {
                updater.Apply(update, item);
                matched = 1;
                modified = 1;
            }
            else if (options != null && options.IsUpsert)
            {
                upsertId = DoUpsert(update);
                modified = 1;
            }
            return new INMUpdateResult(true, matched, modified, upsertId);
        }

        public UpdateResult UpdateOne(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
            => UpdateOne(filter, update, options, cancellationToken);

        public Task<UpdateResult> UpdateOneAsync(FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
            => Task.FromResult(UpdateOne(filter, update, options, cancellationToken));

        public Task<UpdateResult> UpdateOneAsync(IClientSessionHandle session, FilterDefinition<T> filter, UpdateDefinition<T> update, UpdateOptions options = null, CancellationToken cancellationToken = default)
            => UpdateOneAsync(filter, update, options, cancellationToken);

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

        public Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(IClientSessionHandle session, FilterDefinition<T> filter, FindOptions<T, TProjection> options = null, CancellationToken cancellationToken = default)
            => FindAsync(filter, options, cancellationToken);
    }
}
