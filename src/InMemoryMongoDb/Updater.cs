using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using TinyIoC;

namespace InMemoryMongoDb
{
    internal interface IUpdater
    {
        long Apply<T>(UpdateDefinition<T> updateDefinition, BsonDocument doc);
        long Apply<T>(UpdateDefinition<T> updateDefinition, IEnumerable<BsonDocument> docs);
    }

    internal class IMNUpdaterIocInstaller : ITinyIoCInstaller
    {
        public void Install(TinyIoCContainer container)
        {
            container.Register<IUpdater, Updater>();
        }
    }

    internal delegate void BsonUpdater(BsonDocument doc, string name, BsonValue value);

    internal class Updater : IUpdater
    {
        private Dictionary<string, BsonUpdater> updaters = new Dictionary<string, BsonUpdater>
        {
            ["$set"] = DoSet,
            ["$unset"] = DoUnSet,
            ["$inc"] = DoInc,
            ["$mul"] = DoMultiply,
            //["$addToSet"] = DoAddToSet,
            //["$pop"] = DoPop,
            ["$push"] = DoPush
        };

        private static void DoPush(BsonDocument doc, string name, BsonValue value)
        {
            var arrayNode = BsonHelpers.GetDocumentValue(name, doc);
            var isEach = value.IsBsonDocument && value.AsBsonDocument.First().Name == "$each";
            BsonArray arrayValues;
            if (isEach)
                arrayValues = value.AsBsonDocument.First().Value.AsBsonArray;
            else
                arrayValues = BsonArray.Create(new BsonValue[] { value });

            if (arrayNode == BsonNull.Value)
            {
                BsonHelpers.SetDocumentValue(name, doc, arrayValues, isUnset: false);
                return;
            }

            if (!arrayNode.IsBsonArray)
                throw new InMemoryDatabaseException($"Cannot push to element [{arrayNode.BsonType.ToString()}] '{name}', it is not an array");

            var node = arrayNode.AsBsonArray;
            node.AddRange(arrayValues);
        }

        private static void DoMultiply(BsonDocument doc, string name, BsonValue value)
        {
            var currentValue = BsonHelpers.GetDocumentValue(name, doc);
            BsonValue newValue;
            if (currentValue == BsonNull.Value)
                newValue = CastTo(value.BsonType, 0);
            else
                newValue = BsonHelpers.Multiply(currentValue, value);
            DoSet(doc, name, newValue);
        }

        private static BsonValue CastTo(BsonType bsonType, int value)
        {
            switch(bsonType)
            {
                case BsonType.Int32: return (int)0;
                case BsonType.Int64: return (long)0;
                case BsonType.Double: return (double)0;
                case BsonType.Decimal128: return (decimal)0;
                default:
                    throw new InMemoryDatabaseException($"Unable to cast {bsonType.ToString()} to a number");
            }
        }

        private static void DoInc(BsonDocument doc, string name, BsonValue value)
        {
            var currentValue = BsonHelpers.GetDocumentValue(name, doc);
            BsonValue newValue;
            if (currentValue == BsonNull.Value)
                newValue = value;
            else
                newValue = BsonHelpers.Add(currentValue, value);
            DoSet(doc, name, newValue);
        }

        private static void DoUnSet(BsonDocument doc, string name, BsonValue value)
        {
            BsonHelpers.SetDocumentValue(name, doc, value, isUnset: true);
        }

        private static void DoSet(BsonDocument doc, string name, BsonValue value)
        {
            BsonHelpers.SetDocumentValue(name, doc, value, isUnset: false);
        }

        public long Apply<T>(UpdateDefinition<T> updateDefinition, BsonDocument doc)
            => Apply(updateDefinition, Enumerable.Repeat(doc, 1));

        public long Apply<T>(UpdateDefinition<T> updateDefinition, IEnumerable<BsonDocument> docs)
        {
            var serializer = BsonSerializer.SerializerRegistry.GetSerializer<T>();
            var updateDoc = updateDefinition.Render(serializer, BsonSerializer.SerializerRegistry);
            var updateActions = GetActions(updateDoc);
            long updated = 0;
            foreach (var doc in docs)
            {
                foreach (var action in updateActions)
                    action(doc);
                updated++;
            }
            return updated;
        }

        private IEnumerable<Action<BsonDocument>> GetActions(BsonDocument updateDoc)
        {
            foreach(var update in updateDoc)
            {
                if (updaters.TryGetValue(update.Name, out var updater))
                {
                    foreach (var setter in update.Value.AsBsonDocument.Elements)
                        yield return d => updater(d, setter.Name, setter.Value);
                }
                else
                    throw new InMemoryDatabaseException($"Unknown update action '{update.Name}");
            }
        }
    }
}
