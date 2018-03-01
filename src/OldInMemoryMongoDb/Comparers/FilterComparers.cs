using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InMemoryMongoDb.Comparers
{
    internal delegate bool ComparisonOperator(BsonValue lhs, BsonValue rhs);

    internal interface IFilterComparers
    {
        ComparisonOperator GetOperator(string name);
        IEnumerable<ComparisonOperator> ArrayOperators { get; }
    }
    internal class FilterComparers: IFilterComparers
    {
        private readonly Dictionary<string, ComparisonOperator> operators;
        public FilterComparers()
        {
            operators = new Dictionary<string, ComparisonOperator>
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

        public ComparisonOperator GetOperator(string name)
        {
            if (operators.TryGetValue(name, out var comparer))
                return comparer;
            return null;
        }

        public IEnumerable<ComparisonOperator> ArrayOperators { get; } = new ComparisonOperator[] { InComparer, NinComparer, SizeComparer, TypeComparer };

        private static bool TypeComparer(BsonValue lhs, BsonValue rhs)
            => (int)lhs.BsonType == rhs.AsInt32;

        private static bool ExistsComparer(BsonValue lhs, BsonValue rhs)
            => !lhs.IsBsonNull == rhs.AsBoolean;

        private static bool SizeComparer(BsonValue lhs, BsonValue rhs)
            => lhs.IsBsonArray && lhs.AsBsonArray.Count == rhs.AsInt32;

        internal static bool EqComparer(BsonValue lhs, BsonValue rhs)
            => lhs.CompareTo(rhs) == 0;

        internal static bool GtComparer(BsonValue lhs, BsonValue rhs)
            => lhs.CompareTo(rhs) > 0;

        internal static bool GteComparer(BsonValue lhs, BsonValue rhs)
            => lhs.CompareTo(rhs) >= 0;

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
            => lhs.CompareTo(rhs) < 0;

        internal static bool LteComparer(BsonValue lhs, BsonValue rhs)
            => lhs.CompareTo(rhs) <= 0;

        internal static bool NeComparer(BsonValue lhs, BsonValue rhs)
            => lhs.CompareTo(rhs) != 0;

        internal static bool NinComparer(BsonValue lhs, BsonValue rhs)
            => !InComparer(lhs, rhs);
    }
}
