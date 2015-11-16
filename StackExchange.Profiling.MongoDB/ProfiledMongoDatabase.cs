using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using StackExchange.Profiling.MongoDB.Utils;

namespace StackExchange.Profiling.MongoDB
{
    public class ProfiledMongoDatabase : IMongoDatabase
    {
        private readonly IMongoDatabase _source;

        public ProfiledMongoDatabase(IMongoDatabase source)
        {
            _source = source;
        }

        public async Task CreateCollectionAsync(string name, CreateCollectionOptions options = null,
                                                CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            await _source.CreateCollectionAsync(name, options, cancellationToken);
            sw.Stop();

            string commandString = string.Format("{0}.create()", DatabaseNamespace.DatabaseName);

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Command);
        }

        public async Task DropCollectionAsync(string name, CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            await _source.DropCollectionAsync(name, cancellationToken);
            sw.Stop();

            string commandString = string.Format("{0}.drop()", DatabaseNamespace.DatabaseName);

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Command);
        }

        IMongoCollection<TDocument> IMongoDatabase.GetCollection<TDocument>(string name, MongoCollectionSettings settings)
        {
            return new ProfiledMongoCollection<TDocument>(_source.GetCollection<TDocument>(name, settings));
        }

        public async Task<IAsyncCursor<BsonDocument>> ListCollectionsAsync(ListCollectionsOptions options = null,
                                                                           CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _source.ListCollectionsAsync(options, cancellationToken);
            sw.Stop();

            string commandString = string.Format("{0}.list()", DatabaseNamespace.DatabaseName);

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Command);

            var filterDefinition = options != null ? options.Filter : null;
            return new ProfiledMongoCursor<BsonDocument, BsonDocument>(result, null, filterDefinition, null, null, 0, 0);
        }

        public async Task RenameCollectionAsync(string oldName, string newName, RenameCollectionOptions options = null,
                                                CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            await _source.RenameCollectionAsync(oldName, newName, options, cancellationToken);
            sw.Stop();

            string commandString = string.Format("db.{0}.renameCollection(\"{1}\", {2})",
                DatabaseNamespace.DatabaseName,
                oldName,
                newName);

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Command);
        }

        public async Task<TResult> RunCommandAsync<TResult>(Command<TResult> command, ReadPreference readPreference = null,
                                                            CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _source.RunCommandAsync(command, readPreference, cancellationToken);
            sw.Stop();

            string commandString = string.Format("{0}.command()", DatabaseNamespace.DatabaseName);

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Command);

            return result;
        }

        public IMongoClient Client
        {
            get { return _source.Client; }
        }

        public DatabaseNamespace DatabaseNamespace
        {
            get { return _source.DatabaseNamespace; }
        }

        public MongoDatabaseSettings Settings
        {
            get { return _source.Settings; }
        }
    }
}
