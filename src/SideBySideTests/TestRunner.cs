using InMemoryMongoDb;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SideBySideTests
{
    public class TestRunner
    {
        private IEnumerable<Type> GetTests()
        {
            var tests = Assembly.GetAssembly(typeof(TestRunner)).GetTypes()
                .Where(t => t.BaseType == typeof(TestBase));

            return tests;
        }

        public void Run()
        {
            var url = new MongoUrl("mongodb://127.0.0.1:27017/sidebyside");
            var client = new MongoClient(url);
            var logger = new Logger();
            var asserts = new Assert(logger);
            var dbActual = client.GetDatabase(url.DatabaseName);
            var guids = new GuidProvider();

            var testTypes = GetTests();

            foreach(var testType in testTypes)
            {
                var cols = dbActual.ListCollections().ToList();
                foreach (var col in cols)
                {
                    //Console.WriteLine();
                    dbActual.DropCollection(col.Values.First().AsString);
                }
               

                var test = Activator.CreateInstance(testType, new object[] { asserts }) as ITest;
                var actualDocs = RunTest(logger, "Actual", dbActual, test, guids);

                var dbMemory = (new InMemoryClient()).GetDatabase(url.DatabaseName);
                var memoryDocs = RunTest(logger, "Memory", dbMemory, test, guids);

                Compare(logger, actualDocs, memoryDocs);

            }
        }

        private void Compare(ILogger logger, IEnumerable<BsonDocument> actualDocs, IEnumerable<BsonDocument> memoryDocs)
        {
            foreach(var actual in actualDocs)
            {
                var matches = memoryDocs.Where(m => m["TestRowId"] == actual["TestRowId"]).ToList();
                if(matches.Count == 0)
                {
                    logger.Error($"No in memory row for {actual["TestRowid"]}");
                }
                else if(matches.Count > 1)
                {
                    logger.Error($"{matches.Count} memory rows for {actual["TestRowId"]}");
                }
                else
                {
                    if(actual != matches[0])
                    {
                        logger.Error($"Bson Mismatch between\nactual {actual.ToJson()}\nand\nmemory {matches[0].ToJson()}");
                    }
                }
            }
        }

        private static IEnumerable<BsonDocument> RunTest(Logger logger, string dbType, IMongoDatabase dbActual, ITest test, IGuidProvider guids)
        {
            using (var guidIter = guids.Guids())
            {
                logger.Debug($"Starting {dbType} {test.GetType().Name}");
                var docs = test.Run(dbActual, guidIter);
                logger.Debug($"Finished {dbType} {test.GetType().Name}");
                return docs;
            }
        }
    }
}
