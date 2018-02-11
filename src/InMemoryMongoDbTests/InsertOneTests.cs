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
    class InsertOneTests
    {
        [Test]
        public void InsertOne_HasNoId_IdIsGenerated()
        {
            var col = GetCollection<MyEntity>();
            var entity = new MyEntity();
            col.InsertOne(entity);

            entity.Id.Should().NotBe(ObjectId.Empty);
        }

        [Test]
        public void InsertOne_HasId_NoIdGenerated()
        {
            var col = GetCollection<MyEntity>();
            var entity = new MyEntity { Id = ObjectId.GenerateNewId() };
            var id = entity.Id;
            col.InsertOne(entity);

            entity.Id.Should().Be(id);
        }


        private IMongoCollection<T> GetCollection<T>()
        {
            var client = new InMemoryClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<T>("aCollection");

            return col;
        }

    }
}
