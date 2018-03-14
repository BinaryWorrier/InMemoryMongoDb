using InMemoryMongoDb;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


using NUnit.Framework;
using Moq;
using FluentAssertions;

namespace InMemoryMongoDbTests
{
    public class BulkWriteTests
    {
        [Test]
        public void BulkWriteWithInserts()
        {
            var col = GetCollection<MyEntity>();
            var result = col.BulkWrite(new WriteModel<MyEntity>[]
            {
                new InsertOneModel<MyEntity>(new MyEntity { Name = "First" }),
                new InsertOneModel<MyEntity>(new MyEntity { Name = "Second" })
            });

            result.InsertedCount.Should().Be(2);

            var all = col.Find(_ => true).ToList();
            all.Count.Should().Be(2);
            all.Where(e => e.Name == "First").Count().Should().Be(1);
            all.Where(e => e.Name == "Second").Count().Should().Be(1);
        }
        [Test]
        public void BulkWriteWithInsertsAndUpdate()
        {
            var col = GetCollection<MyEntity>();
            var result = col.BulkWrite(new WriteModel<MyEntity>[]
            {
                new InsertOneModel<MyEntity>(new MyEntity { Name = "First" }),
                new InsertOneModel<MyEntity>(new MyEntity { Name = "Second", Number = 1 }),
                new UpdateOneModel<MyEntity>(Builders<MyEntity>.Filter.Where(e => e.Number == 1), Builders<MyEntity>.Update.Set(e => e.Name , "Second Updated"))
            });

            result.InsertedCount.Should().Be(2);
            result.ModifiedCount.Should().Be(1);

            var all = col.Find(_ => true).ToList();
            all.Count.Should().Be(2);
            all.Where(e => e.Name == "First").Count().Should().Be(1);
            all.Where(e => e.Name == "Second Updated").Count().Should().Be(1);
        }

        private IMongoCollection<T> GetCollection<T>()
        {
            var client = new InMemoryClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<T>(typeof(T).Name);

            return col;
        }

    }
}
