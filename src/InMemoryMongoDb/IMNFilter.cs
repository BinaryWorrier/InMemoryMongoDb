using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InMemoryMongoDb
{
    delegate bool LogicalOperator(BsonArray comparisons, BsonDocument doc);
    delegate bool ComparisonOperator(BsonValue lhs, BsonValue rhs);
    class IMNFilter
    {
        private static readonly Dictionary<string, LogicalOperator> logicOperators;
        private static readonly Dictionary<string, ComparisonOperator> compareOperators;
        static IMNFilter()
        {
            logicOperators = new Dictionary<string, LogicalOperator>
            {
                ["$and"] = AndOperator,
                ["$or"] = OrOperator,
                ["$nor"] = NorOperator,
                ["$not"] = NotOperator
            };

            compareOperators = new Dictionary<string, ComparisonOperator>
            {
                ["$eq"] = EqComparer,
                ["$gt"] = GtComparer,
                ["$gte"] = GteComparer,
                ["$in"] = InComparer,
                ["$lt"] = LtComparer,
                ["$lte"] = LteComparer,
                ["$ne"] = NeComparer,
                ["$nin"] = NinComparer,
                ["$size"] = SizeComparer,
                ["$exists"] = ExistsComparer,
                ["$type"] = TypeComparer
            };
        }

        private static bool TypeComparer(BsonValue lhs, BsonValue rhs)
        {
            return (int)lhs.BsonType == rhs.AsInt32;
        }

        private static bool ExistsComparer(BsonValue lhs, BsonValue rhs)
        {
            return !lhs.IsBsonNull == rhs.AsBoolean;
        }

        private static ComparisonOperator[] ArrayOperators = new ComparisonOperator[] { InComparer, NinComparer, SizeComparer, TypeComparer };

        private static bool SizeComparer(BsonValue lhs, BsonValue rhs)
        {
            return lhs.IsBsonArray && lhs.AsBsonArray.Count == rhs.AsInt32;
        }

        internal static bool EqComparer(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) == 0;
        }

        internal static bool GtComparer(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) > 0;
        }

        internal static bool GteComparer(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) >= 0;
        }

        internal static bool InComparer(BsonValue lhs, BsonValue rhs)
        {
            if (lhs.IsBsonArray)
                return lhs.AsBsonArray.Any(l => CheckIsIn(l));
            else
                return CheckIsIn(lhs);

            bool CheckIsIn(BsonValue value) 
                => rhs.AsBsonArray.IndexOf(value) >= 0;
        }

        internal static bool LtComparer(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) < 0;
        }

        internal static bool LteComparer(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) <= 0;
        }

        internal static bool NeComparer(BsonValue lhs, BsonValue rhs)
        {
            return lhs.CompareTo(rhs) != 0;
        }

        internal static bool NinComparer(BsonValue lhs, BsonValue rhs)
        {
            return !InComparer(lhs, rhs);
        }

        public static IEnumerable<BsonDocument> Apply<T>(FilterDefinition<T> filterDefinition, IEnumerable<BsonDocument> docs)
        {
            var ser = MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry.GetSerializer<T>();
            var filterDoc = filterDefinition.Render(ser, MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry);

            foreach (var doc in docs)
                if (IsMatch(filterDoc.Elements, doc))
                    yield return doc;
        }

        private static bool IsMatch(IEnumerable<BsonElement> elements, BsonDocument doc)
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

        private static bool Compare(BsonElement el, BsonDocument doc)
        {
            var name = el.Name;
            var docValue = GetDocumentValue(name, doc);
            if (el.Value.IsBsonDocument)
            {
                var innerDoc = el.Value.AsBsonDocument;
                var firstInner = innerDoc.Elements.First();
                if (compareOperators.TryGetValue(firstInner.Name, out var comparer))
                {
                    return DoCompare(comparer, docValue, firstInner.Value);
                }
                else
                    throw new InMemoryDatabaseException($"Unexpected filter structure '{el}'");
            }
            else
                return DoCompare(EqComparer, docValue, el.Value);
        }

        private static bool DoCompare(ComparisonOperator comparer, BsonValue lhs, BsonValue rhs)
        {
            if (ArrayOperators.Any(op => op == comparer))
                return comparer(lhs, rhs);
            if (lhs.IsBsonArray && !rhs.IsBsonArray)
                return lhs.AsBsonArray.Any(l => comparer(l, rhs));
            //else if (!lhs.IsBsonArray && rhs.IsBsonArray)
            //    return rhs.AsBsonArray.Any(r => comparer(lhs, r));
            else
                return comparer(lhs, rhs);
        }

        private static bool NewDoCompare(ComparisonOperator comparer, BsonValue lhs, BsonValue rhs)
        {
            if (comparer == InComparer || comparer == NinComparer)
            {
                if (rhs.IsBsonArray)
                {
                    var any = rhs.AsBsonArray.Intersect(lhs.AsBsonArray).Any();
                    return (comparer == InComparer && any) || (comparer == NinComparer && !any);
                }
                else
                    return comparer(lhs, rhs);
            }
            else if (lhs.IsBsonArray && !rhs.IsBsonArray)
                return lhs.AsBsonArray.Any(l => comparer(l, rhs));
            //else if (!lhs.IsBsonArray && rhs.IsBsonArray)
            //    return rhs.AsBsonArray.Any(r => comparer(lhs, r));
            else
                return comparer(lhs, rhs);
        }

        private static BsonValue GetDocumentValue(string name, BsonDocument doc)
            => BsonHelpers.GetDocumentValue(name, doc);

        private static bool AndOperator(BsonArray comparisons, BsonDocument doc)
        {
            return comparisons.All(c => IsMatch(c.AsBsonDocument, doc));
        }
        private static bool NotOperator(BsonArray comparisons, BsonDocument doc)
        {
            return !IsMatch(comparisons.AsBsonDocument, doc);
        }

        private static bool OrOperator(BsonArray comparisons, BsonDocument doc)
        {
            return comparisons.Any(c => IsMatch(c.AsBsonDocument, doc));
        }

        /// <summary>
        /// Joins query clauses with a logical NOR returns all documents that fail to match both clauses.
        /// </summary>
        /// <param name="comparisons"></param>
        /// <param name="doc"></param>
        /// <returns></returns>
        private static bool NorOperator(BsonArray comparisons, BsonDocument doc)
        {
            return !comparisons.All(c => IsMatch(c.AsBsonDocument, doc));
        }

    }

}
