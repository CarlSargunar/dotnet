using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using StackExchange.Profiling.MongoDB.Utils;

namespace StackExchange.Profiling.MongoDB
{
    public class ProfiledMongoCursor<TDocument, TProjection> : IAsyncCursor<TProjection>
    {
        private readonly IAsyncCursor<TProjection> _source;
        private readonly IMongoCollection<TDocument> _collection;
        private readonly FilterDefinition<TDocument> _filter;
        private readonly FieldDefinition<TProjection> _fields;
        private readonly SortDefinition<TDocument> _sort;
        private readonly int? _skip;
        private readonly int? _limit;
        private readonly Stopwatch _sw;
        private bool _enumStarted;

        public ProfiledMongoCursor(
            IAsyncCursor<TProjection> source, 
            IMongoCollection<TDocument> collection, 
            FilterDefinition<TDocument> filter, 
            FieldDefinition<TProjection> fields,
            SortDefinition<TDocument> sort,
            int? skip,
            int? limit)
        {
            _source = source;
            _collection = collection;
            _filter = filter;
            _fields = fields;
            _sort = sort;
            _skip = skip;
            _limit = limit;

            _sw = new Stopwatch();
        }
        
        public void Dispose()
        {
            _source.Dispose();
        }

        public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = new CancellationToken())
        {
            if (!_enumStarted)
            {
                _enumStarted = true;
                _sw.Start();
            }

            var result = await _source.MoveNextAsync(cancellationToken);

            if (!result)
            {
                _sw.Stop();

                OnEnumerationEnded(new EnumerationEndedEventArgs { Elapsed = _sw.Elapsed });
            }

            return result;
        }

        public IEnumerable<TProjection> Current
        {
            get { return _source.Current; }
        }


        protected virtual void OnEnumerationEnded(EnumerationEndedEventArgs enumerationEndedEventArgs)
        {
            var commandStringBuilder = new StringBuilder(1024);

            if (_collection != null)
            {
                commandStringBuilder.Append(_collection.CollectionNamespace.CollectionName);
            }
            commandStringBuilder.Append(".find(");

            if (_filter != null)
                commandStringBuilder.Append("query");

            if (_fields != null)
                commandStringBuilder.Append(",fields");

            commandStringBuilder.Append(")");

            if (_sort != null)
                commandStringBuilder.Append(".sort(orderBy)");

            if (_skip != 0)
                commandStringBuilder.AppendFormat(".skip({0})", _skip);

            if (_limit != 0)
                commandStringBuilder.AppendFormat(".limit({0})", _limit);

            if (_filter != null)
                commandStringBuilder.AppendFormat("\nquery = {0}", _filter.ToBsonDocument());

            if (_fields != null)
                commandStringBuilder.AppendFormat("\nfields = {0}", _fields.ToBsonDocument());

            if (_sort != null)
                commandStringBuilder.AppendFormat("\norderBy = {0}", _sort.ToBsonDocument());

            // TODO: implement other options printout if needed

            string commandString = commandStringBuilder.ToString();

            ProfilerUtils.AddMongoTiming(commandString, (long)enumerationEndedEventArgs.Elapsed.TotalMilliseconds, ExecuteType.Read);
        }

        public class EnumerationEndedEventArgs : EventArgs
        {
            public TimeSpan Elapsed { get; set; }
        }
    }
}
