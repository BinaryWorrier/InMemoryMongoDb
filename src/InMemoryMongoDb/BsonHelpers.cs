using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Text;

namespace InMemoryMongoDb
{
    static class BsonHelpers
    {
        public static BsonValue GetDocumentValue(string name, BsonDocument doc)
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

    }
}
