using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace InMemoryMongoDb
{
    internal class InMemoryFilteredCollection<T, TDerivedDocument> : InMemoryCollection<TDerivedDocument>, IFilteredMongoCollection<TDerivedDocument> 
        where TDerivedDocument : T
    {
        public InMemoryFilteredCollection(IMongoDatabase db, VanillaCollection vCol, IFilter whereFilter, IUpdater updater, FilterDefinition<TDerivedDocument> filterDefinition)
            :base(db, vCol, whereFilter, updater)
        {
            Filter = filterDefinition;
        }

        protected override IEnumerable<BsonDocument> ApplyFilter(FilterDefinition<TDerivedDocument> filter, long? limit, long? skip)
        {
            var byTypeFilter = Builders<TDerivedDocument>.Filter.And(
                Builders<TDerivedDocument>.Filter.OfType<TDerivedDocument>(),
                filter);
            return base.ApplyFilter(byTypeFilter, limit, skip);
        }

        public FilterDefinition<TDerivedDocument> Filter { get; }

   }
}