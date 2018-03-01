using MongoDB.Bson;
using MoreLinq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InMemoryMongoDb
{
    static class BsonHelpers
    {
        
        public static BsonValue OldGetDocumentValue(string name, BsonDocument doc)
        {
            var graph = name.Split('.');
            BsonValue node = doc;
            try
            {
                foreach (var next in graph)
                    if (node.IsBsonDocument)
                    {
                        var nodeDoc = node.AsBsonDocument;
                        if (nodeDoc.TryGetElement(next, out var element))
                            node = element.Value;
                        else
                            return BsonValue.Create(null);
                    }
                    else if (node.IsBsonArray)
                    {
                        var nodeArray = node.AsBsonArray;
                        if (int.TryParse(next, out var index) && index >= 0 && index < nodeArray.Count)
                            node = nodeArray[index];
                        else
                            return BsonValue.Create(null);
                    }
                    else
                        throw new InMemoryDatabaseException($"Can not get value for '{name}' in {doc}");
                return node;
            }
            catch (KeyNotFoundException)
            {
                return BsonValue.Create(null);
            }
        }

        public static BsonValue GetDocumentValue(string name, BsonDocument doc)
        {
            BsonValue lastNode = doc;
            var fullGraph = name.Split('.').Select(n => ParseName(n)).ToList();
            for (int i = 0; i < fullGraph.Count - 1; i++)
            {
                var current = fullGraph[i];
                if (current.IsName)
                {
                    var nextNode = lastNode.AsBsonDocument[current.Name];
                    if (nextNode == BsonNull.Value)
                        throw new InMemoryDatabaseException($"Can not get value for '{name}' in {doc}");
                    lastNode = nextNode;
                }
                else if (current.Index >= 0 && current.Index < lastNode.AsBsonArray.Count)
                    lastNode = lastNode.AsBsonArray[current.Index];
                else
                    throw new InMemoryDatabaseException($"Can not get value for '{name}' in {doc}");
            }
            var last = fullGraph.Last();
            if (last.IsName)
                return lastNode.AsBsonDocument.GetValue(last.Name);
            else if (last.Index >= 0 && last.Index < lastNode.AsBsonArray.Count)
                return lastNode.AsBsonArray[last.Index];
            else
                return BsonNull.Value;
        }

        private static (string Name, int Index, bool IsName) ParseName(string name)
        {
            var isInt = int.TryParse(name, out var i);
            return (name, i, !isInt);
        }
        public static void SetDocumentValue(string name, BsonDocument doc, BsonValue value, bool isUnset)
        {
            BsonValue lastNode = doc;
            var fullGraph = name.Split('.').Select(n => ParseName(n)).ToList();
            for (int i = 0; i < fullGraph.Count - 1; i++)
            {
                var current = fullGraph[i];
                if (current.IsName)
                {
                    var nextNode = lastNode.AsBsonDocument[current.Name];
                    if (nextNode == BsonNull.Value)
                        lastNode.AsBsonDocument.Set(current.Name, nextNode = new BsonDocument());
                    lastNode = nextNode;
                }
                else
                    lastNode = lastNode.AsBsonArray[current.Index];
            }
            var last = fullGraph.Last();
            if (last.IsName)
                if (isUnset)
                    lastNode.AsBsonDocument.Remove(last.Name);
            else
                    lastNode.AsBsonDocument.Set(last.Name, value);
            else
                
                lastNode.AsBsonArray[last.Index] = isUnset ? BsonNull.Value :  value;
        }

        private enum NumbersToScale
        {
            Int,
            Long,
            Double,
            Decimal
        }
        private static Dictionary<BsonType, (NumbersToScale Scale, Func<BsonValue, object> Accessor, Func<object, object> Cast)> bsonAccessorMap = new Dictionary<BsonType, (NumbersToScale Scale, Func<BsonValue, object> Accessor, Func<object, object> Cast)>
        {
            [BsonType.Int32] = (NumbersToScale.Int, v => v.AsInt32, i => i),
            [BsonType.Int64] = (NumbersToScale.Long, v => v.AsInt64, i => (long)i),
            [BsonType.Double] = (NumbersToScale.Double, v => v.AsDouble, i => (double)i),
            [BsonType.Decimal128] = (NumbersToScale.Decimal, v => v.AsDecimal, i => (decimal)i)
        };

        private static NumbersToScale Max(NumbersToScale a, NumbersToScale b)
        {
            return (NumbersToScale)Math.Max((int)a, (int)b);
        }
        private static T[] ToArray<T>(params T[] items)
            => items;

        private static (object First, object Second) RaiseToMinimum(BsonValue first, BsonValue second)
        {
            var firstInfo = bsonAccessorMap[first.BsonType];
            var secondInfo = bsonAccessorMap[second.BsonType];

            var castor = ToArray(firstInfo, secondInfo).MaxBy(n => n.Scale).Cast;

            return (castor(firstInfo.Accessor(first)), castor(secondInfo.Accessor(second)));
        }

        public static BsonValue Add(BsonValue a, BsonValue b)
        {
            var (x, y) = RaiseToMinimum(a, b);
            if (x is int i) return i + (int)y;
            else if (x is long l) return l + (long)y;
            else if (x is double dbl) return dbl + (double)y;
            else if (x is Decimal dec) return dec + (Decimal)y;
            else
                throw new InMemoryDatabaseException($"Unable to add bson values ({a.BsonType.ToString()}){a} and ({b.BsonType.ToString()}){b}");
        }
        public static BsonValue Multiply(BsonValue a, BsonValue b)
        {
            var (x, y) = RaiseToMinimum(a, b);
            if (x is int i) return i * (int)y;
            else if (x is long l) return l * (long)y;
            else if (x is double dbl) return dbl * (double)y;
            else if (x is Decimal dec) return dec * (Decimal)y;
            else
                throw new InMemoryDatabaseException($"Unable to multiply bson values ({a.BsonType.ToString()}){a} and ({b.BsonType.ToString()}){b}");
        }

        //public static void OldSetDocumentValue(string name, BsonDocument doc, BsonValue value)
        //{
        //    var graph = name.Split('.');
        //    BsonValue node = doc;
        //    try
        //    {
        //        var isNewDocument;
        //        var last = graph.Last();
        //        foreach (var next in graph.TakeAllButLast())
        //            if (node.IsBsonDocument)
        //            {
        //                var nodeDoc = node.AsBsonDocument;
        //                if (nodeDoc.TryGetElement(next, out var element))
        //                    node = element.Value;
        //                else
        //                    node = BsonNull.Value;
        //                if (node == BsonNull.Value)
        //                {
        //                    nodeDoc.Set(next, node = new BsonDocument());
        //                    isNewDocument = true;
        //                }
        //            }
        //            else if (node.IsBsonArray)
        //            {
        //                var nodeArray = node.AsBsonArray;

        //                if (int.TryParse(next, out var index) && nodeArray != BsonNull.Value && index >= 0 && index < nodeArray.Count)
        //                    node = nodeArray[index];
        //            }
        //            else 
        //                throw new InMemoryDatabaseException($"Can not set value for '{name}' in {doc}");

        //        if (node.IsBsonDocument)
        //            node.AsBsonDocument.Set(graph.Last(), value);
        //        else
        //            node.AsBsonArray[int.Parse(graph.Last())] = value;
        //    }
        //    catch (KeyNotFoundException)
        //    {
        //    }
        //}
    }

}
