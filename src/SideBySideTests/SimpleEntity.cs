using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Text;

namespace SideBySideTests
{
    public class SimpleEntity
    {
        public ObjectId Id { get; set; }
        public Guid TestRowId { get; set; }
        public string Name { get; set; }
        public int Int { get; set; }
    }
}
