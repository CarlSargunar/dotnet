using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using StackExchange.Profiling.Storage;

namespace StackExchange.Profiling.MongoDB
{
    /// <summary>
    /// Understands how to store a <see cref="MiniProfiler"/> to a MongoDb database.
    /// </summary>
    public class MongoDbStorage : DatabaseStorageBase
    {
        #region Collections

        private IMongoDatabase _db;
        private IMongoDatabase Db
        {
            get
            {
                if (_db == null)
                {
                    var client = new MongoClient(ConnectionString);
                    _db = client.GetDatabase("MiniProfiler");
                }
                return _db;
            }
        }

        private IMongoCollection<MiniProfilerPoco> _profilers;
        private IMongoCollection<MiniProfilerPoco> Profilers
        {
            get
            {
                if (_profilers == null)
                {
                    _profilers = Db.GetCollection<MiniProfilerPoco>("profilers");
                }
                return _profilers;
            }
        }

        private IMongoCollection<TimingPoco> _timings;
        private IMongoCollection<TimingPoco> Timings
        {
            get
            {
                if (_timings == null)
                {
                    _timings = Db.GetCollection<TimingPoco>("timings");
                }
                return _timings;
            }
        }

        private IMongoCollection<CustomTimingPoco> _customTimings;
        private IMongoCollection<CustomTimingPoco> CustomTimings
        {
            get
            {
                if (_customTimings == null)
                {
                    _customTimings = Db.GetCollection<CustomTimingPoco>("customtimings");
                }
                return _customTimings;
            }
        }

        private IMongoCollection<ClientTimingPoco> _clientTimings;
        private IMongoCollection<ClientTimingPoco> ClientTimings
        {
            get
            {
                if (_clientTimings == null)
                {
                    _clientTimings = Db.GetCollection<ClientTimingPoco>("clienttimings");
                }
                return _clientTimings;
            }
        }

        #endregion

        /// <summary>
        /// Returns a new <see cref="MongoDbStorage"/>. MongoDb connection string will default to "mongodb://localhost"
        /// </summary>
        public MongoDbStorage(string connectionString)
            : base(connectionString)
        {
        }

        /// <summary>
        /// Stores <param name="profiler"/> to MongoDB under its <see cref="MiniProfiler.Id"/>; 
        /// stores all child Timings and SqlTimings to their respective tables.
        /// </summary>
        public override async void Save(MiniProfiler profiler)
        {
            var miniProfilerPoco = new MiniProfilerPoco
            {
                Id = profiler.Id,
                RootTimingId = profiler.Root != null ? profiler.Root.Id : (Guid?) null,
                Name = profiler.Name,
                Started = profiler.Started,
                DurationMilliseconds = (double) profiler.DurationMilliseconds,
                User = profiler.User,
                HasUserViewed = profiler.HasUserViewed,
                MachineName = profiler.MachineName,
                CustomLinksJson = profiler.CustomLinksJson,
                ClientTimingsRedirectCounts = profiler.ClientTimings != null ? profiler.ClientTimings.RedirectCount : (int?) null
            };

            var result = await Profilers.ReplaceOneAsync(Builders<MiniProfilerPoco>.Filter.Eq(c => c.Id, miniProfilerPoco.Id), miniProfilerPoco);

            if (result.ModifiedCount == 0)
            {
                await SaveTiming(profiler.Root);
            }

            await SaveClientTimings(profiler);
        }

        private async Task SaveTiming(Timing timing)
        {
            var rootTiming = new TimingPoco
            {
                Id = timing.Id,
                ParentTimingId = timing.IsRoot ? (Guid?)null : timing.ParentTiming.Id,
                Name = timing.Name,
                StartMilliseconds = (double)timing.StartMilliseconds,
                DurationMilliseconds = (double)timing.DurationMilliseconds
            };

            await Timings.InsertOneAsync(rootTiming);

            if (timing.HasChildren)
            {
                foreach (var child in timing.Children)
                {
                    await SaveTiming(child);
                }
            }

            if (timing.HasCustomTimings)
            {
                foreach (var customTimingsKV in timing.CustomTimings)
                {
                    await SaveCustomTimings(timing, customTimingsKV);
                }
            }
        }

        private async Task SaveClientTimings(MiniProfiler profiler)
        {
            if (profiler.ClientTimings == null || profiler.ClientTimings.Timings == null || !profiler.ClientTimings.Timings.Any())
                return;

            profiler.ClientTimings.Timings.ForEach(x =>
            {
                x.MiniProfilerId = profiler.Id;
                x.Id = Guid.NewGuid();
            });

            foreach (var clientTiming in profiler.ClientTimings.Timings)
            {
                var clientTimingPoco = new ClientTimingPoco
                {
                    Id = clientTiming.Id,
                    MiniProfilerId = clientTiming.MiniProfilerId,
                    Name = clientTiming.Name,
                    Start = (double)clientTiming.Start,
                    Duration = (double)clientTiming.Duration
                };

                await ClientTimings.ReplaceOneAsync(Builders<ClientTimingPoco>.Filter.Eq(c => c.Id, clientTiming.Id), clientTimingPoco);
            }
        }

        private async Task SaveCustomTimings(Timing timing, KeyValuePair<string, List<CustomTiming>> customTimingsKV)
        {
            var key = customTimingsKV.Key;
            var value = customTimingsKV.Value;

            foreach (var customTiming in value)
            {
                var customTimingPoco = new CustomTimingPoco
                {
                    Id = customTiming.Id,
                    Key = key,
                    TimingId = timing.Id,
                    CommandString = customTiming.CommandString,
                    ExecuteType = customTiming.ExecuteType,
                    StackTraceSnippet = customTiming.StackTraceSnippet,
                    StartMilliseconds = customTiming.StartMilliseconds,
                    DurationMilliseconds = customTiming.DurationMilliseconds,
                    FirstFetchDurationMilliseconds = customTiming.FirstFetchDurationMilliseconds
                };

                await CustomTimings.InsertOneAsync(customTimingPoco);
            }
        }

        /// <summary>
        /// Loads the MiniProfiler identifed by 'id' from the database.
        /// </summary>
        public override MiniProfiler Load(Guid id)
        {
            var profilerPoco = Profilers.Find(Builders<MiniProfilerPoco>.Filter.Eq(poco => poco.Id, id)).FirstOrDefaultAsync().Result;
            var miniProfiler = ProfilerPocoToProfiler(profilerPoco);

            if (miniProfiler != null)
            {
                var rootTiming = miniProfiler.RootTimingId.HasValue
                    ? LoadTiming(miniProfiler.RootTimingId.Value)
                    : null;

                if (rootTiming != null)
                {
                    miniProfiler.Root = rootTiming;
                }

                miniProfiler.ClientTimings = LoadClientTimings(miniProfiler);
            }

            return miniProfiler;
        }

        private Timing LoadTiming(Guid id)
        {
            var timingPoco = Timings.Find(Builders<TimingPoco>.Filter.Eq(poco => poco.Id, id)).FirstOrDefaultAsync().Result;
            var timing = TimingPocoToTiming(timingPoco);

            if (timing != null)
            {
                timing.Children = LoadChildrenTimings(timing.Id);
                timing.CustomTimings = LoadCustomTimings(timing.Id);
            }

            return timing;
        }

        private List<Timing> LoadChildrenTimings(Guid parentId)
        {
            var childrenTimings =
                    Timings.Find(Builders<TimingPoco>.Filter.Eq(poco => poco.ParentTimingId, parentId))
                        .Sort(Builders<TimingPoco>.Sort.Ascending(poco => poco.StartMilliseconds))
                        .ToListAsync().Result
                        .Select(TimingPocoToTiming)
                        .ToList();

            childrenTimings.ForEach(timing =>
            {
                timing.Children = LoadChildrenTimings(timing.Id);
                timing.CustomTimings = LoadCustomTimings(timing.Id);
            });

            return childrenTimings;
        }

        private Dictionary<string, List<CustomTiming>> LoadCustomTimings(Guid timingId)
        {
            var customTimingPocos = CustomTimings
                .Find(Builders<CustomTimingPoco>.Filter.Eq(poco => poco.TimingId, timingId))
                .ToListAsync().Result;

            return customTimingPocos
                .GroupBy(poco => poco.Key)
                .ToDictionary(grp => grp.Key,
                    grp => grp.OrderBy(poco => poco.StartMilliseconds)
                        .Select(CustomTimingPocoToCustomTiming).ToList());
        }

        private ClientTimings LoadClientTimings(MiniProfiler profiler)
        {
            var timings = ClientTimings
                .Find(Builders<ClientTimingPoco>.Filter.Eq(poco => poco.MiniProfilerId, profiler.Id))
                .ToListAsync().Result
                .Select(ClientTimingPocoToClientTiming)
                .ToList();

            timings.ForEach(timing => timing.MiniProfilerId = profiler.Id);

            if (timings.Any() || profiler.ClientTimingsRedirectCount.HasValue)
                return new ClientTimings
                {
                    Timings = timings,
                    RedirectCount = profiler.ClientTimingsRedirectCount ?? 0
                };

            return null;
        }

        #region Mapping

        private static MiniProfiler ProfilerPocoToProfiler(MiniProfilerPoco profilerPoco)
        {
            if (profilerPoco == null)
                return null;

#pragma warning disable 618
            var miniProfiler = new MiniProfiler
#pragma warning restore 618
            {
                Id = profilerPoco.Id,
                MachineName = profilerPoco.MachineName,
                User = profilerPoco.User,
                HasUserViewed = profilerPoco.HasUserViewed,
                Name = profilerPoco.Name,
                Started = profilerPoco.Started,
                RootTimingId = profilerPoco.RootTimingId,
                DurationMilliseconds = (decimal) profilerPoco.DurationMilliseconds
            };

            return miniProfiler;
        }

        private static Timing TimingPocoToTiming(TimingPoco timingPoco)
        {
            if (timingPoco == null)
                return null;

#pragma warning disable 618
            return new Timing
#pragma warning restore 618
            {
                Id = timingPoco.Id,
                Name = timingPoco.Name,
                StartMilliseconds = (decimal) timingPoco.StartMilliseconds,
                DurationMilliseconds = (decimal) timingPoco.DurationMilliseconds,
            };
        }

        private static CustomTiming CustomTimingPocoToCustomTiming(CustomTimingPoco customTimingPoco)
        {
#pragma warning disable 618
            return new CustomTiming
#pragma warning restore 618
            {
                CommandString = customTimingPoco.CommandString,
                DurationMilliseconds = customTimingPoco.DurationMilliseconds,
                FirstFetchDurationMilliseconds = customTimingPoco.FirstFetchDurationMilliseconds,
                StartMilliseconds = customTimingPoco.StartMilliseconds,
                ExecuteType = customTimingPoco.ExecuteType,
                StackTraceSnippet = customTimingPoco.StackTraceSnippet
            };
        }

        private static ClientTimings.ClientTiming ClientTimingPocoToClientTiming(ClientTimingPoco clientTimingPoco)
        {
            return new ClientTimings.ClientTiming
            {
                Id = clientTimingPoco.Id,
                Duration = (decimal) clientTimingPoco.Duration,
                Name = clientTimingPoco.Name,
                Start = (decimal) clientTimingPoco.Start
            };
        }

        #endregion

        /// <summary>
        /// Sets the profiler as unviewed
        /// </summary>
        public override void SetUnviewed(string user, Guid id)
        {
            Profilers.UpdateOneAsync(Builders<MiniProfilerPoco>.Filter.Eq("_id", id.ToString()),
                Builders<MiniProfilerPoco>.Update.Set(poco => poco.HasUserViewed, false));
        }

        /// <summary>
        /// Sets the profiler as view
        /// </summary>
        public override void SetViewed(string user, Guid id)
        {
            Profilers.UpdateOneAsync(Builders<MiniProfilerPoco>.Filter.Eq("_id", id.ToString()),
                Builders<MiniProfilerPoco>.Update.Set(poco => poco.HasUserViewed, true));
        }

        /// <summary>
        /// Returns a list of <see cref="MiniProfiler.Id"/>s that haven't been seen by <paramref name="user"/>.
        /// </summary>
        /// <param name="user">User identified by the current <see cref="MiniProfiler.Settings.UserProvider"/>.</param>
        public override List<Guid> GetUnviewedIds(string user)
        {
            var query = Builders<MiniProfilerPoco>.Filter.And(
                    Builders<MiniProfilerPoco>.Filter.Eq(p => p.User, user),
                    Builders<MiniProfilerPoco>.Filter.Eq(p => p.HasUserViewed, false));
            var guids = Profilers.Find(query).ToListAsync().Result.Select(p => p.Id).ToList();
            return guids;
        }

        public override IEnumerable<Guid> List(int maxResults, DateTime? start = null, DateTime? finish = null, ListResultsOrder orderBy = ListResultsOrder.Descending)
        {
            FilterDefinition<MiniProfilerPoco> query = null;

            if (start != null)
            {
                query = Builders<MiniProfilerPoco>.Filter.And(Builders<MiniProfilerPoco>.Filter.Gt("Started", (DateTime)start));
            }
            if (finish != null)
            {
                query = Builders<MiniProfilerPoco>.Filter.And(Builders<MiniProfilerPoco>.Filter.Lt("Started", (DateTime)finish));
            }

            var profilers = Profilers.Find(query).ToListAsync().Result.Take(maxResults);

            profilers = orderBy == ListResultsOrder.Descending
                ? profilers.OrderByDescending(p => p.Started)
                : profilers.OrderBy(p => p.Started);

            return profilers.Select(p => p.Id);
        }

        #region Poco Classes

        //In order to use Guids as the Id in MongoDb we have to use a strongly typed class and the [BsonId] attribute on the Id.  Otherwise Mongo defaults to ObjectIds which cannot be cast to Guids.

        class MiniProfilerPoco
        {
            [BsonId]
            public Guid Id { get; set; }
            public string Name { get; set; }
            public DateTime Started { get; set; }
            public string MachineName { get; set; }
            public string User { get; set; }
            public Guid? RootTimingId { get; set; }
            public double DurationMilliseconds { get; set; }
            public string CustomLinksJson { get; set; }
            public int? ClientTimingsRedirectCounts { get; set; }
            public bool HasUserViewed { get; set; }
        }

        class TimingPoco
        {
            [BsonId]
            public Guid Id { get; set; }
            public Guid? ParentTimingId { get; set; }
            public string Name { get; set; }
            public double StartMilliseconds { get; set; }
            public double DurationMilliseconds { get; set; }
        }

        class CustomTimingPoco
        {
            [BsonId]
            public Guid Id { get; set; }
            public string Key { get; set; }
            public Guid TimingId { get; set; }
            public string CommandString { get; set; }
            public string ExecuteType { get; set; }
            public string StackTraceSnippet { get; set; }
            public decimal StartMilliseconds { get; set; }
            public decimal? DurationMilliseconds { get; set; }
            public decimal? FirstFetchDurationMilliseconds { get; set; }
        }

        class ClientTimingPoco
        {
            [BsonId]
            public Guid Id { get; set; }
            public Guid MiniProfilerId { get; set; }
            public string Name { get; set; }
            public double Start { get; set; }
            public double Duration { get; set; }
        }

        #endregion
    }
}
