using InMemoryMongoDb.Comparers;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TinyIoC;

namespace InMemoryMongoDb
{
    internal interface IFilter
    {
        IEnumerable<BsonDocument> Apply<T>(FilterDefinition<T> filterDefinition, IEnumerable<BsonDocument> docs);
    }

    internal class IMNFilterIocInstaller : ITinyIoCInstaller
    {
        public void Install(TinyIoCContainer container)
        {
            container.Register<IFilter, IMNFilter>();
        }
    }

    internal delegate bool LogicalOperator(BsonArray comparisons, BsonDocument doc);
    internal class IMNFilter: IFilter
    {
        private readonly IFilterComparers filterComparers;
        private readonly ComparisonOperator EqComparer; 
        public IMNFilter(IFilterComparers filterComparers)
        {
            this.filterComparers = filterComparers ?? throw new ArgumentNullException(nameof(filterComparers));
            EqComparer = filterComparers.GetOperator("$eq");
            logicOperators = new Dictionary<string, LogicalOperator>
            {
                ["$and"] = AndOperator,
                ["$or"] = OrOperator,
                ["$nor"] = NorOperator,
                ["$not"] = NotOperator
            };
        }

        private readonly Dictionary<string, LogicalOperator> logicOperators;

        public IEnumerable<BsonDocument> Apply<T>(FilterDefinition<T> filterDefinition, IEnumerable<BsonDocument> docs)
        {
            var ser = MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry.GetSerializer<T>();
            var filterDoc = filterDefinition.Render(ser, MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry);

            foreach (var doc in docs)
                if (IsMatch(filterDoc.Elements, doc))
                    yield return doc;
        }

        private bool IsMatch(IEnumerable<BsonElement> elements, BsonDocument doc)
        {
            // Assume it's an and and all elements are equals.
            foreach (var el in elements)
            {
                if (logicOperators.TryGetValue(el.Name, out var op))
                {
                    if (!op((el.Value.AsBsonArray), doc))
                        return false;
                }
                else
                {
                    if (!Compare(el, doc))
                        return false;
                }
            }
            return true;
        }

        private  bool Compare(BsonElement el, BsonDocument doc)
        {
            var name = el.Name;
            var docValue = GetDocumentValue(name, doc);
            if (el.Value.IsBsonDocument)
            {
                var innerDoc = el.Value.AsBsonDocument;
                var firstInner = innerDoc.Elements.First();
                var comparer=filterComparers.GetOperator(firstInner.Name);
                if(comparer == null)
                    throw new InMemoryDatabaseException($"Unexpected filter structure '{el}'");

                return DoCompare(comparer, docValue, firstInner.Value);
            }
            else
                return DoCompare(EqComparer, docValue, el.Value);
        }

        private bool DoCompare(ComparisonOperator comparer, BsonValue lhs, BsonValue rhs)
        {
            if (filterComparers.ArrayOperators.Any(op => op == comparer))
                return comparer(lhs, rhs);
            if (lhs.IsBsonArray && !rhs.IsBsonArray)
                return lhs.AsBsonArray.Any(l => comparer(l, rhs));
            else
                return comparer(lhs, rhs);
        }

        private static BsonValue GetDocumentValue(string name, BsonDocument doc)
            => BsonHelpers.GetDocumentValue(name, doc);

        private bool AndOperator(BsonArray comparisons, BsonDocument doc) => comparisons.All(c => IsMatch(c.AsBsonDocument, doc));

        private bool NotOperator(BsonArray comparisons, BsonDocument doc) => !IsMatch(comparisons.AsBsonDocument, doc);

        private bool OrOperator(BsonArray comparisons, BsonDocument doc) 
            => comparisons.Any(c => IsMatch(c.AsBsonDocument, doc));

        /// <summary>
        /// Joins query clauses with a logical NOR returns all documents that fail to match both clauses.
        /// </summary>
        /// <param name="comparisons"></param>
        /// <param name="doc"></param>
        /// <returns></returns>
        private bool NorOperator(BsonArray comparisons, BsonDocument doc) => !comparisons.All(c => IsMatch(c.AsBsonDocument, doc));

    }

}
