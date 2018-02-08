using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;
using Moq;
using FluentAssertions;
using MongoDB.Bson;
using InMemoryMongoDb;
using MongoDB.Driver;

namespace InMemoryMongoDbTests
{
    [TestFixture]
    class InMemoryCollectionTests
    {

        [Test]
        public void Count_AppliesFilter_CountsAll()
        {
            var filter = Filter.Where(_ => true);
            var col = GetCollection(new MyEntity { }, new MyEntity { });
            col.Count(filter).Should().Be(2);
        }

        [Test]
        public void Count_AppliesFilter_CountsMatch()
        {
            var filter = Filter.Where(e => e.Name == "Mark" || e.Name == "Noel");
            var col = GetCollection(
                new MyEntity { Name = "Mark" },
                new MyEntity { Name = "Joe" },
                new MyEntity { Name = "Noel" }
                );
            col.Count(filter).Should().Be(2);
        }

        private IMongoCollection<MyEntity> GetCollection(params MyEntity [] entities)
        {
            var client = new InMemoryClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<MyEntity>("myEntities");

            col.InsertMany(entities);
            return col;
        }
        FilterDefinitionBuilder<MyEntity> Filter => Builders<MyEntity>.Filter;


        [Test]
        public void Distinct()
        {
            var filter = Filter.Where(_ => true);
            var col = GetCollection(
                new MyEntity { Name = "Mark" },
                new MyEntity { Name = "Joe" },
                new MyEntity { Name = "Mark" }
                );

            var values = col.Distinct(e => e.Name, filter).ToList();
            values.Count.Should().Be(2);
            values.Should().Contain("Mark");
            values.Should().Contain("Joe");


        }

    }
}
