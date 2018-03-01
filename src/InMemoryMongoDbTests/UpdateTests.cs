using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;
using Moq;
using FluentAssertions;
using MongoDB.Driver;
using InMemoryMongoDb;
using MongoDB.Bson;

namespace InMemoryMongoDbTests
{
    [TestFixture]
    class UpdateTests
    {
        private IMongoCollection<T> GetCollection<T>()
        {
            var client = new InMemoryClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<T>("aCollection");

            return col;
        }

        private List<BsonDocument> ToBson(params MyEntity[] entities)
            => entities.Select(e => e.ToBsonDocument()).ToList();
        private UpdateDefinitionBuilder<MyEntity> Updater => Builders<MyEntity>.Update;
        [Test]
        public void Apply_PlainSets_NewValuesShouldExist()
        {
            //var col = GetCollection<MyEntity>();

            var update = Updater
                .Set(e => e.Name, "Mark")
                .Set(e => e.Number, 10);

            var docs = ToBson(new MyEntity { });

            var updater = new Updater();
            updater.Apply(update, docs);

            docs[0].GetValue("Name").Should().Be("Mark");
            docs[0].GetValue("Number").Should().Be(10);
        }

        [Test]
        public void Apply_FieldOnChildWhenChildExists_FieldChanged()
        {
            var update = Updater
                .Set(e => e.TheThings.ThingA, "A");

            var docs = ToBson(new MyEntity { TheThings = new MyEntitiesThings { ThingA = "AnA" } });

            var updater = new Updater();
            updater.Apply(update, docs);

            docs[0]["TheThings"]["ThingA"].Should().Be("A");
        }
        [Test]
        public void Apply_FieldOnChildWhenChildNOTExists_FieldChanged()
        {
            var update = Updater
                .Set(e => e.TheThings.ThingA, "A");

            var docs = ToBson(new MyEntity { });

            var updater = new Updater();
            updater.Apply(update, docs);

            docs[0]["TheThings"]["ThingA"].Should().Be("A");
        }

        [Test]
        public void Apply_ReplaceArrayElement_ValueIsChanged()
        {
            var update = Updater
                .Set(e => e.Books[1], "A");

            var docs = ToBson(new MyEntity { Books = new List<string> { "X", "Y", "Z" } });

            var updater = new Updater();
            updater.Apply(update, docs);

            docs[0]["Books"][1].Should().Be("A");
        }

        [Test]
        public void Apply_ReplaceArrayWhenArrayIsNull_ValueIsChanged()
        {
            var update = Updater
                .Set(e => e.Books, new List<string> { "A" });

            var docs = ToBson(new MyEntity { });

            var updater = new Updater();
            updater.Apply(update, docs);

            docs[0]["Books"].IsBsonArray.Should().BeTrue();
            docs[0]["Books"][0].Should().Be("A");
        }

        [Test]
        public void Apply_UnsetSimple_RemovesItem()
        {
            //var col = GetCollection<MyEntity>();

            var update = Updater
                .Unset(e => e.Name);

            var docs = ToBson(new MyEntity { Name = "Mark"});

            var updater = new Updater();
            updater.Apply(update, docs);

            docs[0].Any(e => e.Name == "Mark").Should().BeFalse();
        }

        [TestCase(null, 1, ExpectedResult = 1)]
        [TestCase("1", 2, ExpectedResult = 3)]
        [TestCase("5", 9, ExpectedResult = 14)]
        [TestCase("5", -9, ExpectedResult = -4)]
        public long Apply_IncInts_AddsAndSetsCorrectType(string entityValue, int incAmount)
        {
            var update = Updater
                .Inc(e => e.NullableLong, incAmount);

            var entity = new MyEntity();
            if (!string.IsNullOrWhiteSpace(entityValue))
                entity.NullableLong = int.Parse(entityValue);
            var docs = ToBson(entity);

            var updater = new Updater();
            updater.Apply(update, docs);

            return docs[0]["NullableLong"].AsInt64;
        }

        [TestCase(null, 2, ExpectedResult = 0)]
        [TestCase("2", 3, ExpectedResult = 6)]
        [TestCase("5", 9, ExpectedResult = 45)]
        [TestCase("5", -9, ExpectedResult = -45)]
        public long Apply_MultipleInts_MultipliesAndSetsCorrectType(string entityValue, int by)
        {
            var update = Updater
                .Mul(e => e.NullableLong, by);

            var entity = new MyEntity();
            if (!string.IsNullOrWhiteSpace(entityValue))
                entity.NullableLong = int.Parse(entityValue);
            var docs = ToBson(entity);

            var updater = new Updater();
            updater.Apply(update, docs);

            return docs[0]["NullableLong"].AsInt64;
        }

        [Test]
        public void Apply_PushToEmpty()
        {
            var update = Updater.Push(e => e.Books, "My Book");
            var entity = new MyEntity();

            var docs = ToBson(entity);

            var updater = new Updater();
            updater.Apply(update, docs);

            var array = docs[0]["Books"].AsBsonArray;
            array.Count.Should().Be(1);
            array[0].Should().Be("My Book");

        }

        [Test]
        public void Apply_PushToExisting()
        {
            var update = Updater.Push(e => e.Books, "My Book");
            var entity = new MyEntity { Books = new List<string> { "Your Book" } };

            var docs = ToBson(entity);

            var updater = new Updater();
            updater.Apply(update, docs);

            var array = docs[0]["Books"].AsBsonArray;
            array.Count.Should().Be(2);
            array[0].Should().Be("Your Book");
            array[1].Should().Be("My Book");

        }

        [Test]
        public void Apply_PushEachToEmpty()
        {
            var update = Updater.PushEach(e => e.Books, new string[] { "A", "B", "C" });
            var entity = new MyEntity();

            var docs = ToBson(entity);

            var updater = new Updater();
            updater.Apply(update, docs);

            var array = docs[0]["Books"].AsBsonArray;
            array.Count.Should().Be(3);
            array[0].Should().Be("A");
            array[1].Should().Be("B");
            array[2].Should().Be("C");
        }

        [Test]
        public void Apply_PushEachToExisting()
        {
            var update = Updater.PushEach(e => e.Books, new string[] { "A", "B", "C" });
            var entity = new MyEntity { Books = new List<string> { "X", "Y", "Z" } };

            var docs = ToBson(entity);

            var updater = new Updater();
            updater.Apply(update, docs);

            var array = docs[0]["Books"].AsBsonArray;
            array.Count.Should().Be(6);
            array[0].Should().Be("X");
            array[1].Should().Be("Y");
            array[2].Should().Be("Z");
            array[3].Should().Be("A");
            array[4].Should().Be("B");
            array[5].Should().Be("C");
        }
    }
}
