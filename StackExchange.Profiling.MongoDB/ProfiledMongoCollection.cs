using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using StackExchange.Profiling.MongoDB.Utils;

namespace StackExchange.Profiling.MongoDB
{
    public class ProfiledMongoCollection<TDefaultDocument> : IMongoCollection<TDefaultDocument>
    {
        private readonly IMongoCollection<TDefaultDocument> _collection;

        public ProfiledMongoCollection(IMongoCollection<TDefaultDocument> collection)
        {
            _collection = collection;
        }

        public async Task<IAsyncCursor<TResult>> AggregateAsync<TResult>(PipelineDefinition<TDefaultDocument, TResult> pipeline, AggregateOptions options = null,
                                            CancellationToken cancellationToken = new CancellationToken())
        {
            var source = await _collection.AggregateAsync(pipeline, options, cancellationToken);
            return new ProfiledMongoCursor<TDefaultDocument, TResult>(source, this, null, null, null, 0, 0);
        }

        public async Task<BulkWriteResult<TDefaultDocument>> BulkWriteAsync(IEnumerable<WriteModel<TDefaultDocument>> requests, BulkWriteOptions options = null, CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.BulkWriteAsync(requests, options, cancellationToken);
            sw.Stop();

            string commandString = string.Format("db.{0}.bulkWrite(requests, options)", Name);

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Create);

            return result;
        }

        public async Task<long> CountAsync(FilterDefinition<TDefaultDocument> filter, CountOptions options = null, CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.CountAsync(filter, options, cancellationToken);
            sw.Stop();

            string commandString = filter != null
                ? string.Format("db.{0}.count(query)\n\nquery = {1}", _collection.CollectionNamespace.CollectionName, RenderFilter(filter))
                : string.Format("db.{0}.count()", _collection.CollectionNamespace.CollectionName);

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Read);

            return result;
        }

        private string RenderFilter(FilterDefinition<TDefaultDocument> filter)
        {
            var documentSerializer = BsonSerializer.SerializerRegistry.GetSerializer<TDefaultDocument>();
            return filter.Render(documentSerializer, BsonSerializer.SerializerRegistry).ToJson();
        }

        public async Task<DeleteResult> DeleteManyAsync(FilterDefinition<TDefaultDocument> filter, CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.DeleteManyAsync(filter, cancellationToken);
            sw.Stop();

            var commandStringBuilder = new StringBuilder(1024);

            commandStringBuilder.AppendFormat("db.{0}.remove", Name);

            if (filter != null)
            {
                commandStringBuilder.Append("(");
                commandStringBuilder.AppendFormat("query");
                
                commandStringBuilder.Append(")");

                commandStringBuilder.AppendFormat("\nquery = {0}", filter.ToBsonDocument());
            }

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Create);

            return result;
        }

        public async Task<DeleteResult> DeleteOneAsync(FilterDefinition<TDefaultDocument> filter, CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.DeleteManyAsync(filter, cancellationToken);
            sw.Stop();

            var commandStringBuilder = new StringBuilder(1024);

            commandStringBuilder.AppendFormat("db.{0}.remove", Name);

            if (filter != null)
            {
                commandStringBuilder.Append("(");
                commandStringBuilder.AppendFormat("query");

                commandStringBuilder.Append(", true");

                commandStringBuilder.Append(")");

                commandStringBuilder.AppendFormat("\nquery = {0}", filter.ToBsonDocument());
            }
            else
            {
                commandStringBuilder.Append("({}, true)");
            }

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Create);

            return result;
        }


        public async Task<IAsyncCursor<TField>> DistinctAsync<TField>(FieldDefinition<TDefaultDocument, TField> field, FilterDefinition<TDefaultDocument> filter, DistinctOptions options = null,
                                          CancellationToken cancellationToken = new CancellationToken())
        {
            var source = await _collection.DistinctAsync(field, filter, options, cancellationToken);
            return new ProfiledMongoCursor<TDefaultDocument, TField>(source, this, filter, null, null, 0, 0);
        }

        public async Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(FilterDefinition<TDefaultDocument> filter, FindOptions<TDefaultDocument, TProjection> options = null, CancellationToken cancellationToken = new CancellationToken())
        {
            var source = await _collection.FindAsync(filter, options, cancellationToken);

            if (options == null)
            {
                options = new FindOptions<TDefaultDocument, TProjection>();
            }

            return new ProfiledMongoCursor<TDefaultDocument, TProjection>(source, this, filter, null, options.Sort, options.Skip, options.Limit);
        }

        public async Task<TProjection> FindOneAndDeleteAsync<TProjection>(FilterDefinition<TDefaultDocument> filter, FindOneAndDeleteOptions<TDefaultDocument, TProjection> options = null,
                                                       CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.FindOneAndDeleteAsync(filter, options, cancellationToken);
            sw.Stop();

            var commandStringBuilder = new StringBuilder(1024);
            commandStringBuilder.AppendFormat("db.{0}.findAndModify(query, sort, remove, fields)", Name);

            if (filter != null)
                commandStringBuilder.AppendFormat("\nquery = {0}", filter.ToBsonDocument());
            else
                commandStringBuilder.Append("\nquery = null");

            if (options.Sort != null)
                commandStringBuilder.AppendFormat("\nsort = {0}", options.Sort.ToBsonDocument());
            else
                commandStringBuilder.Append("\nsort = null");

            commandStringBuilder.AppendFormat("\nremove = true");

            if (options.Projection != null)
                commandStringBuilder.AppendFormat("\nfields = {0}", options.Projection.ToBsonDocument());
            else
                commandStringBuilder.Append("\nfields = null");

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Update);

            return result;
        }

        public async Task<TProjection> FindOneAndReplaceAsync<TProjection>(FilterDefinition<TDefaultDocument> filter, TDefaultDocument replacement, FindOneAndReplaceOptions<TDefaultDocument, TProjection> options = null,
                                                        CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.FindOneAndReplaceAsync(filter, replacement, options, cancellationToken);
            sw.Stop();

            var commandStringBuilder = new StringBuilder(1024);
            commandStringBuilder.AppendFormat("db.{0}.findAndModify(query, sort, update, new, fields, upsert)", Name);

            if (filter != null)
                commandStringBuilder.AppendFormat("\nquery = {0}", filter.ToBsonDocument());
            else
                commandStringBuilder.Append("\nquery = null");

            if (options.Sort != null)
                commandStringBuilder.AppendFormat("\nsort = {0}", options.Sort.ToBsonDocument());
            else
                commandStringBuilder.Append("\nsort = null");
            
            if (options.Projection != null)
                commandStringBuilder.AppendFormat("\nfields = {0}", options.Projection.ToBsonDocument());
            else
                commandStringBuilder.Append("\nfields = null");

            commandStringBuilder.AppendFormat("\nupsert = {0}", options.IsUpsert ? "true" : "false");

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Update);

            return result;
        }

        public async Task<TProjection> FindOneAndUpdateAsync<TProjection>(FilterDefinition<TDefaultDocument> filter, UpdateDefinition<TDefaultDocument> update, FindOneAndUpdateOptions<TDefaultDocument, TProjection> options = null,
                                                       CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.FindOneAndUpdateAsync(filter, update, options, cancellationToken);
            sw.Stop();

            var commandStringBuilder = new StringBuilder(1024);
            commandStringBuilder.AppendFormat("db.{0}.findAndModify(query, sort, update, new, fields, upsert)", Name);

            if (filter != null)
                commandStringBuilder.AppendFormat("\nquery = {0}", filter.ToBsonDocument());
            else
                commandStringBuilder.Append("\nquery = null");

            if (options.Sort != null)
                commandStringBuilder.AppendFormat("\nsort = {0}", options.Sort.ToBsonDocument());
            else
                commandStringBuilder.Append("\nsort = null");

            if (update != null)
                commandStringBuilder.AppendFormat("\nupdate = {0}", update.ToBsonDocument());
            else
                commandStringBuilder.Append("\nupdate = null");

            if (options.Projection != null)
                commandStringBuilder.AppendFormat("\nfields = {0}", options.Projection.ToBsonDocument());
            else
                commandStringBuilder.Append("\nfields = null");

            commandStringBuilder.AppendFormat("\nupsert = {0}", options.IsUpsert ? "true" : "false");

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Update);

            return result;
        }

        public Task InsertOneAsync(TDefaultDocument document, CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = _collection.InsertOneAsync(document, cancellationToken);
            sw.Stop();

            var commandStringBuilder = new StringBuilder(512);

            commandStringBuilder.AppendFormat("db.{0}.insert(", Name);

            // handle ensureIndex specially
            if (Name == "system.indexes")
                commandStringBuilder.AppendFormat("{0}", document.ToBsonDocument());
            else
                commandStringBuilder.Append("<document>");

            commandStringBuilder.Append(")");

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Create);

            return result;
        }

        public Task InsertManyAsync(IEnumerable<TDefaultDocument> documents, InsertManyOptions options = null, CancellationToken cancellationToken = new CancellationToken())
        {
            var documentsList = documents.Cast<object>().ToList();

            var sw = new Stopwatch();

            sw.Start();
            var result = _collection.InsertManyAsync(documents, options, cancellationToken);
            sw.Stop();

            var commandStringBuilder = new StringBuilder(512);

            commandStringBuilder.AppendFormat("db.{0}.insert(", Name);

            if (documentsList.Count > 1)
                commandStringBuilder.AppendFormat("<{0} documents>", documentsList.Count);
            else
            {
                // handle ensureIndex specially
                if (Name == "system.indexes")
                    commandStringBuilder.AppendFormat("{0}", documentsList.First().ToBsonDocument());
                else
                    commandStringBuilder.Append("<document>");
            }

            commandStringBuilder.Append(")");

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Create);

            return result;
        }

        public async Task<IAsyncCursor<TResult>> MapReduceAsync<TResult>(BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<TDefaultDocument, TResult> options = null,
                                            CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.MapReduceAsync(map, reduce, options, cancellationToken);
            sw.Stop();

            string commandString = string.Format("db.{0}.mapReduce(<map function>, <reduce function>, options)", Name);

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Read);

            return result;
        }

        public async Task<ReplaceOneResult> ReplaceOneAsync(FilterDefinition<TDefaultDocument> filter, TDefaultDocument replacement, UpdateOptions options = null,
                                    CancellationToken cancellationToken = new CancellationToken())
        {

            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.ReplaceOneAsync(filter, replacement, options, cancellationToken);
            sw.Stop();

            string commandString = string.Format("db.{0}.replace(filter, replacement, options)", Name);

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Read);

            return result;
        }

        public async Task<UpdateResult> UpdateManyAsync(FilterDefinition<TDefaultDocument> filter, UpdateDefinition<TDefaultDocument> update, UpdateOptions options = null,
                                    CancellationToken cancellationToken = new CancellationToken())
        {
            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.UpdateManyAsync(filter, update, options, cancellationToken);
            sw.Stop();

            var commandStringBuilder = new StringBuilder(1024);

            commandStringBuilder.AppendFormat("db.{0}.update(query, update", Name);

            var optionsList = new List<string>();

            if ((options.IsUpsert))
                optionsList.Add("upsert: true");
            
            optionsList.Add("multi: true");

            if (optionsList.Any())
                commandStringBuilder.AppendFormat("{{ {0} }}", string.Join(", ", optionsList));

            commandStringBuilder.Append(")");

            if (filter != null)
                commandStringBuilder.AppendFormat("\nquery = {0}", filter.ToBsonDocument());
            else
                commandStringBuilder.Append("\nquery = {}");

            if (update != null)
                commandStringBuilder.AppendFormat("\nupdate = {0}", update.ToBsonDocument());
            else
                commandStringBuilder.Append("\nupdate = {}");

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Update);

            return result;
        }

        public string Name
        {
            get { return _collection.CollectionNamespace.CollectionName; }
        }

        public async Task<UpdateResult> UpdateOneAsync(FilterDefinition<TDefaultDocument> filter, UpdateDefinition<TDefaultDocument> update, UpdateOptions options = null,
                                   CancellationToken cancellationToken = new CancellationToken())
        {

            var sw = new Stopwatch();

            sw.Start();
            var result = await _collection.UpdateOneAsync(filter, update, options, cancellationToken);
            sw.Stop();

            var commandStringBuilder = new StringBuilder(1024);

            commandStringBuilder.AppendFormat("db.{0}.update(query, update", Name);

            var optionsList = new List<string>();

            if (options != null && options.IsUpsert)
                optionsList.Add("upsert: true");
            
            if (optionsList.Any())
                commandStringBuilder.AppendFormat("{{ {0} }}", string.Join(", ", optionsList));

            commandStringBuilder.Append(")");

            if (filter != null)
                commandStringBuilder.AppendFormat("\nquery = {0}", filter.ToBsonDocument());
            else
                commandStringBuilder.Append("\nquery = {}");

            if (update != null)
                commandStringBuilder.AppendFormat("\nupdate = {0}", update.ToBsonDocument());
            else
                commandStringBuilder.Append("\nupdate = {}");

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, sw.ElapsedMilliseconds, ExecuteType.Update);

            return result;
        }

        public IMongoCollection<TDefaultDocument> WithReadPreference(ReadPreference readPreference)
        {
            return _collection.WithReadPreference(readPreference);
        }

        public IMongoCollection<TDefaultDocument> WithWriteConcern(WriteConcern writeConcern)
        {
            return _collection.WithWriteConcern(writeConcern);
        }

        public CollectionNamespace CollectionNamespace
        {
            get { return _collection.CollectionNamespace; }
        }

        public IMongoDatabase Database
        {
            get { return _collection.Database; }
        }

        public IBsonSerializer<TDefaultDocument> DocumentSerializer
        {
            get { return _collection.DocumentSerializer; }
        }

        public IMongoIndexManager<TDefaultDocument> Indexes
        {
            get { return _collection.Indexes; }
        }

        public MongoCollectionSettings Settings
        {
            get { return _collection.Settings; }
        }
    }
}
