using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using StackExchange.Profiling.MongoDB;

namespace SampleWeb.Data
{
    public class MongoDataRepository
    {
        public string MongoUrl { get; private set; }
        public string DbName { get; set; }

        public MongoDataRepository(string mongoUrl, string dbName)
        {
            MongoUrl = mongoUrl;
            DbName = dbName;
        }

        private MongoClient _client;
        public MongoClient Client
        {
            get
            {
                if (_client == null)
                {
                    _client = new MongoClient(MongoUrl);
                }

                return _client;
            }
        }
        
        private IMongoDatabase _database;
        public IMongoDatabase Database
        {
            get
            {
                if (_database == null)
                {
                    _database = Client.GetDatabase(DbName);
                }
                return _database;
            }
        }

        private IMongoCollection<BsonDocument> _fooCollection;
        public IMongoCollection<BsonDocument> FooCollection
        {
            get
            {
                if (_fooCollection == null)
                {
                    _fooCollection = Database.GetCollection<BsonDocument>("foo");
                }
                return _fooCollection;
            }
        }

        private MongoDB.Driver.IMongoCollection<MongoDB.Bson.BsonDocument> _barCollection;
        public IMongoCollection<BsonDocument> BarCollection
        {
            get
            {
                if (_barCollection == null)
                {
                    _barCollection = Database.GetCollection<BsonDocument>("bar");
                }
                return _barCollection;
            }
        }

        private IMongoCollection<BazzItem> _bazzCollection;
        public IMongoCollection<BazzItem> BazzCollection
        {
            get
            {
                if (_bazzCollection == null)
                {
                    _bazzCollection = Database.GetCollection<BazzItem>("bazz");
                }
                return _bazzCollection;
            }
        }
    }

    public class BazzItem
    {
        [BsonId]
        public ObjectId Id { get; set; }

        public int SomeRandomInt { get; set; }

        public double SomeRandomDouble { get; set; }

        public DateTime CurrentTimestamp { get; set; }
    }
}
