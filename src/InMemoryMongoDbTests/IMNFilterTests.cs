using FluentAssertions;
using InMemoryMongoDb;
using InMemoryMongoDb.Comparers;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace InMemoryMongoDbTests
{
    [TestFixture]
    public class IMNFilterTests
    {
        [Test]
        public void EqComparer_WithEqual_True()
        {
            var filter = Builders<MyEntity>.Filter.Where(d => d.Name == "Mark");
            var entites = new[] { (new MyEntity { Name = "Mark" }) };

            var docs = Apply(filter, entites);
            docs.Count().Should().Be(1);
        }

        private IEnumerable<BsonDocument> Apply<T>(FilterDefinition<T> filter, IEnumerable<T> entities)
        {
            var comparers = new FilterComparers();
            return (new IMNFilter(comparers)).Apply(filter, entities.Select(e => e.ToBsonDocument()), null, null);
        }

        [Test]
        public void EqComparer_WithNotEqual_False()
        {
            var filter = Builders<MyEntity>.Filter.Where(d => d.Name == "Marcus");
            var entites = new[] { (new MyEntity { Name = "Mark" }) };
            var docs = Apply(filter, entites);

            docs.Count().Should().Be(0);
        }

        [TestCase(1, 0, ExpectedResult = true)]
        [TestCase(0, 0, ExpectedResult = false)]
        [TestCase(-1, 0, ExpectedResult = false)]
        public bool GtComparer(int lhs, int rhs)
        {
            var filter = Builders<MyEntity>.Filter.Where(d => d.Number > rhs);
            var entites = new[] { (new MyEntity { Number = lhs }) };
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase(1, 0, ExpectedResult = true)]
        [TestCase(0, 0, ExpectedResult = true)]
        [TestCase(-1, 0, ExpectedResult = false)]
        public bool GteComparer(int lhs, int rhs)
        {
            var filter = Builders<MyEntity>.Filter.Where(d => d.Number >= rhs);
            var entites = new[] { (new MyEntity { Number = lhs }) };
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase(1, 0, ExpectedResult = false)]
        [TestCase(0, 0, ExpectedResult = false)]
        [TestCase(-1, 0, ExpectedResult = true)]
        public bool LtComparer(int lhs, int rhs)
        {
            var filter = Builders<MyEntity>.Filter.Where(d => d.Number < rhs);
            var entites = new[] { (new MyEntity { Number = lhs })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase(1, 0, ExpectedResult = false)]
        [TestCase(0, 0, ExpectedResult = true)]
        [TestCase(-1, 0, ExpectedResult = true)]
        public bool LteComparer(int lhs, int rhs)
        {
            var filter = Builders<MyEntity>.Filter.Where(d => d.Number <= rhs);
            var entites = new[] { (new MyEntity { Number = lhs }) };
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("Mark,Joe,Noel", "Mark", ExpectedResult = true)]
        [TestCase("Mark,Joe,Noel", "Joe", ExpectedResult = true)]
        [TestCase("Mark,Joe,Noel", "Noel", ExpectedResult = true)]
        [TestCase("Mark,Joe,Noel", "Rick", ExpectedResult = false)]
        public bool InComparer(string rhs, string lhs)
        {
            var filter = Builders<MyEntity>.Filter.In(d => d.Name, rhs.Split(','));
            var entites = new[] { (new MyEntity { Name = lhs })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("Mark,Joe,Noel", "Mark", ExpectedResult = false)]
        [TestCase("Mark,Joe,Noel", "Joe", ExpectedResult = false)]
        [TestCase("Mark,Joe,Noel", "Noel", ExpectedResult = false)]
        [TestCase("Mark,Joe,Noel", "Rick", ExpectedResult = true)]
        public bool NotInComparer(string rhs, string lhs)
        {
            var filter = Builders<MyEntity>.Filter.Nin(d => d.Name, rhs.Split(','));
            var entites = new[] { (new MyEntity { Name = lhs }) };
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("Mark", "Mark", ExpectedResult = false)]
        [TestCase("Mark", "Joe", ExpectedResult = true)]
        public bool NeComparer(string entityValue, string queryValue)
        {
            var filter = Builders<MyEntity>.Filter.Ne(d => d.Name, queryValue);
            var entites = new[] { (new MyEntity { Name = entityValue }) };
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("Mark", "Mark", ExpectedResult = true)]
        [TestCase("Mark", "Joe", ExpectedResult = false)]
        public bool EqComparer(string entityValue, string queryValue)
        {
            var filter = Builders<MyEntity>.Filter.Eq(d => d.Name, queryValue);
            var entites = new[] { (new MyEntity { Name = entityValue })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("Mark", "Mark,Joe,Noel", ExpectedResult = true)]
        [TestCase("Rick", "Mark,Joe,Noel", ExpectedResult = false)]
        public bool AnyEqComparer(string queryValue, string entityValue)
        {
            var filter = Builders<MyEntity>.Filter.AnyEq(d => d.Books, queryValue);
            var entites = new[] { (new MyEntity { Books = entityValue.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A", "B,C,D", ExpectedResult = true)]
        [TestCase("B", "B,C,D", ExpectedResult = true)]
        [TestCase("C", "B,C,D", ExpectedResult = true)]
        [TestCase("D", "B,C,D", ExpectedResult = false)]
        [TestCase("E", "B,C,D", ExpectedResult = false)]
        public bool AnyGtComparer(string rhs, string lhs)
        {
            var filter = Builders<MyEntity>.Filter.AnyGt(d => d.Books, rhs);
            var entites = new[] { (new MyEntity { Books = lhs.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A", "B,C,D", ExpectedResult = true)]
        [TestCase("B", "B,C,D", ExpectedResult = true)]
        [TestCase("C", "B,C,D", ExpectedResult = true)]
        [TestCase("D", "B,C,D", ExpectedResult = true)]
        [TestCase("E", "B,C,D", ExpectedResult = false)]
        public bool AnyGteComparer(string rhs, string lhs)
        {
            var filter = Builders<MyEntity>.Filter.AnyGte(d => d.Books, rhs);
            var entites = new[] { (new MyEntity { Books = lhs.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A,B", "B,C,D", ExpectedResult = true)]
        [TestCase("B", "B,C,D", ExpectedResult = true)]
        [TestCase("C", "B,C,D", ExpectedResult = true)]
        [TestCase("D", "B,C,D", ExpectedResult = true)]
        [TestCase("A,E", "B,C,D", ExpectedResult = false)]
        public bool AnyInComparer(string rhs, string lhs)
        {
            var filter = Builders<MyEntity>.Filter.AnyIn(d => d.Books, rhs.Split(','));
            var entites = new[] { (new MyEntity { Books = lhs.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A", "B,C,D", ExpectedResult = false)]
        [TestCase("B", "B,C,D", ExpectedResult = false)]
        [TestCase("C", "B,C,D", ExpectedResult = true)]
        [TestCase("D", "B,C,D", ExpectedResult = true)]
        [TestCase("E", "B,C,D", ExpectedResult = true)]
        public bool AnyLtComparer(string rhs, string lhs)
        {
            var filter = Builders<MyEntity>.Filter.AnyLt(d => d.Books, rhs);
            var entites = new[] { (new MyEntity { Books = lhs.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A", "B,C,D", ExpectedResult = false)]
        [TestCase("B", "B,C,D", ExpectedResult = true)]
        [TestCase("C", "B,C,D", ExpectedResult = true)]
        [TestCase("D", "B,C,D", ExpectedResult = true)]
        [TestCase("E", "B,C,D", ExpectedResult = true)]
        public bool AnyLteComparer(string rhs, string lhs)
        {
            var filter = Builders<MyEntity>.Filter.AnyLte(d => d.Books, rhs);
            var entites = new[] { (new MyEntity { Books = lhs.Split(',').ToList() }) };
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("Mark", "Noel", ExpectedResult = true)]
        [TestCase("Mark", "Mark", ExpectedResult = false)]
        public bool AnyNeComparer(string queryValue, string entityValue)
        {
            var filter = Builders<MyEntity>.Filter.Ne(d => d.Name, queryValue);
            var entites = new[] { (new MyEntity { Name = entityValue })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A", "B,C,D", ExpectedResult = true)]
        [TestCase("B", "B,C,D", ExpectedResult = false)]
        [TestCase("C", "B,C,D", ExpectedResult = false)]
        [TestCase("D", "B,C,D", ExpectedResult = false)]
        [TestCase("E", "B,C,D", ExpectedResult = true)]
        public bool NinComparer(string lhs, string rhs)
        {
            var filter = Builders<MyEntity>.Filter.Nin(d => d.Name, rhs.Split(','));
            var entites = new[] { (new MyEntity { Name = lhs }) };
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("Mark", "Noel", ExpectedResult = true)]
        [TestCase("Mark", "Mark", ExpectedResult = false)]
        public bool NotComparer(string queryValue, string entityValue)
        {
            var filter = Builders<MyEntity>.Filter.Not(Builders<MyEntity>.Filter.Where(d => d.Name == queryValue));
            var entites = new[] { (new MyEntity { Name = entityValue })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }
        FilterDefinitionBuilder<MyEntity> Filter => Builders<MyEntity>.Filter;

        [TestCase("Mark", "Noel", ExpectedResult = false)]
        [TestCase("Mark", "Mark", ExpectedResult = true)]
        public bool OrComparer(string queryValue, string entityValue)
        {
            var filter = Filter.Or(Filter.Where(d => d.Name == queryValue), Filter.Where(d => d.Name == "nod"));
            var entites = new[] { (new MyEntity { Name = entityValue })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }


        [Test]
        public void OfType_ReturnsCorrectObject()
        {
            var filter = Filter.OfType<MyDerivedEntity>(d => d.Name == "Mark");
            var entites = new[] { (new MyEntity { Name = "Mark" }),
                                  (new MyDerivedEntity { Name = "Mark", ExtraStuff = "My Stuff" }) };
            var docs = Apply(filter, entites);
            var doc = docs.First();
            
            var derived = BsonSerializer.Deserialize<MyDerivedEntity>(doc);

            derived.ExtraStuff.Should().Be("My Stuff");
        }

        [TestCase("A", 1, ExpectedResult = true)]
        [TestCase("A", 2, ExpectedResult = false)]
        [TestCase("A,B", 2, ExpectedResult = true)]
        public bool Size(string items, int size)
        {
            var filter = Filter.Size(d => d.Books, size);
            var entites = new[] { (new MyEntity { Books = items.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A", 1, ExpectedResult = false)]
        [TestCase("A", 2, ExpectedResult = false)]
        [TestCase("A,B", 1, ExpectedResult = true)]
        public bool SizeGt(string items, int size)
        {
            var filter = Filter.SizeGt(d => d.Books, size);
            var entites = new[] { (new MyEntity { Books = items.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A", 1, ExpectedResult = true)]
        [TestCase("A", 2, ExpectedResult = false)]
        [TestCase("A,B", 1, ExpectedResult = true)]
        public bool SizeGte(string items, int size)
        {
            var filter = Filter.SizeGte(d => d.Books, size);
            var entites = new[] { (new MyEntity { Books = items.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A", 1, ExpectedResult = false)]
        [TestCase("A", 2, ExpectedResult = true)]
        [TestCase("A,B", 1, ExpectedResult = false)]
        public bool SizeLt(string items, int size)
        {
            var filter = Filter.SizeLt(d => d.Books, size);
            var entites = new[] { (new MyEntity { Books = items.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }

        [TestCase("A", 1, ExpectedResult = true)]
        [TestCase("A", 2, ExpectedResult = true)]
        [TestCase("A,B", 1, ExpectedResult = false)]
        public bool SizeLte(string items, int size)
        {
            var filter = Filter.SizeLte(d => d.Books, size);
            var entites = new[] { (new MyEntity { Books = items.Split(',').ToList() })};
            var docs = Apply(filter, entites);

            return docs.Any();
        }


        [Test]
        public void TypeFilter_Array_IsArrayType()
        {
            var filter = Filter.Type(d => d.Books, BsonType.Array);
            var entites = new[] { (new MyEntity { Books = "A,B".Split(',').ToList() }) };
            var docs = Apply(filter, entites);

            docs.Any().Should().BeTrue();
        }

        [Test]
        public void TypeFilter_Array_IsNotInt32()
        {
            var filter = Filter.Type(d => d.Books, BsonType.Int32);
            var entites = new[] { (new MyEntity { Books = "A,B".Split(',').ToList() })};
            var docs = Apply(filter, entites);

            docs.Any().Should().BeFalse();
        }

        [Test]
        public void TypeFilter_Array_IsNotNull()
        {
            var filter = Filter.Type(d => d.Books, BsonType.Null);
            var entites = new[] { (new MyEntity { Books = "A,B".Split(',').ToList() })};
            var docs = Apply(filter, entites);

            docs.Any().Should().BeFalse();
        }

        [Test]
        public void TypeFilter_Array_IsNull()
        {
            var filter = Filter.Type(d => d.Books, BsonType.Null);
            var entites = new[] { (new MyEntity {  })};
            var docs = Apply(filter, entites);

            docs.Any().Should().BeTrue();
        }

        [Test]
        public void TypeFilter_String_IsArrayType()
        {
            var filter = Filter.Type(d => d.Name, BsonType.String);
            var entites = new[] { (new MyEntity { Name = "A,B" })};
            var docs = Apply(filter, entites);

            docs.Any().Should().BeTrue();
        }

        [Test]
        public void TypeFilter_String_IsNotInt32()
        {
            var filter = Filter.Type(d => d.Name, BsonType.Int32);
            var entites = new[] { (new MyEntity { Name = "A,B" })};
            var docs = Apply(filter, entites);

            docs.Any().Should().BeFalse();
        }

        [Test]
        public void TypeFilter_String_IsNotNull()
        {
            var filter = Filter.Type(d => d.Name, BsonType.Null);
            var entites = new[] { (new MyEntity { Name = "A,B" })};
            var docs = Apply(filter, entites);

            docs.Any().Should().BeFalse();
        }

        [Test]
        public void TypeFilter_String_IsNull()
        {
            var filter = Filter.Type(d => d.Name, BsonType.Null);
            var entites = new[] { (new MyEntity { }) };
            var docs = Apply(filter, entites);
            docs.Any().Should().BeTrue();
        }


        [TestCase(true, ExpectedResult = true)]
        [TestCase(false, ExpectedResult = false)]
        public bool Where_PassingSimplyTrueOrFalse(bool booleanValue)
        {
            var filter = Filter.Where(_ => booleanValue);
            var entites = new[] { (new MyEntity { })};
            var docs = Apply(filter, entites);
            return docs.Any();
        }
    }
}
