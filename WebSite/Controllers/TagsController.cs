using StackOverflowTagServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http;
using Server.Infrastructure;
using System.Diagnostics;
using System.Globalization;
using StackOverflowTagServer.DataStructures;
using Microsoft.ApplicationInsights;

namespace Server.Controllers
{
    [ActionWebApiFilter]
    public class TagsController : ApiController
    {
        // GET: api/Tags
        public object Get()
        {
            var telemetry = new TelemetryClient();
            try
            {
                //telemetry.TrackEvent("API-Tags-Get()");
                var tagServer = WebApiApplication.TagServer.Value;
                return GetAPIInfo(tagServer);
            }
            catch (Exception ex)
            {
                Trace.Write(ex);
                telemetry.TrackException(ex); //, properties, measurements);
                return new { Error = ex.ToString().Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries) };
            }
        }

        private object GetAPIInfo(TagServer tagServer)
        {
            return new
            {
                SampleUrls = new
                {
                    BasicQueries = new Dictionary<string, string>
                            {
                                { Request.RequestUri.ToString() + "/c%23",
                                    "c# questions (have to escape \"c#\" -> \"c%23\"), defaults to QueryType.ViewCount, pageSize = 50, skip = 0" },
                                { Request.RequestUri.ToString() +
                                    "/c%23?Type=LastActivityDate&PageSize=25", "25 Most Recent c# questions" },
                                { Request.RequestUri.ToString() +
                                    "/c%23?Type=Score&PageSize=1&Skip=71993", "Lowest scoring c# question" },
                                { Request.RequestUri.ToString() +
                                    "/.net?Type=AnswerCount&PageSize=1", "The .NET question with the most answers" },
                            }.ToArray(),
                    RelatedTagQueries = new Dictionary<string, string>
                    {
                    }.ToArray(),
                },
                SetupMessages = TagServer.Messages,
                Top50Tags = tagServer.AllTags
                                        .Take(50),
                Bottom50Tags = tagServer.AllTags
                                        .OrderBy(t => t.Value)
                                        .Take(50)
            };
        }

        // GET: api/Tags/5
        //public object Get(string tag,
        //                  QueryType type = QueryType.ViewCount,
        //                  int pageSize = 50,
        //                  int skip = 0)
        public object Get(string tag)
        {
            // Couldn't get Web API to play nice with "c%23" ("c#") and query parameters?!?
            var queryStringPairs = HttpContext.Current.Request.QueryString.ToPairs();
            var type = QueryStringOrDefaultEnum(queryStringPairs, "type", QueryType.ViewCount);
            var pageSize = QueryStringOrDefaultInt(queryStringPairs, "pageSize", 50);
            var skip = QueryStringOrDefaultInt(queryStringPairs, "skip", 0);

            var timer = Stopwatch.StartNew();
            var results = WebApiApplication.TagServer.Value.Query(type, tag, pageSize, skip);
            timer.Stop();

            return new
            {
                Statistics = new {
                    Elapsed = timer.Elapsed,
                    ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                    Count = results.Count,
                    TotalCount = WebApiApplication.TagServer.Value.TotalCount(type, tag)
                },
                DEBUGGING = new
                {
                    Tag = tag,
                    QueryType = type.ToString(),
                    PageSize = pageSize,
                    Skip = skip,
                    HttpContext.Current.Request.Path,
                    HttpContext.Current.Request.RawUrl,
                    QueryString = HttpContext.Current.Request.QueryString
                                            .ToPairs()
                                            .ToDictionary(p => p.Key, p => p.Value)
                },
                Results = results,
            };
        }

        public object GetWithNotOtherTag(string tag)
        {
            // Couldn't get Web API to play nice with "c%23" ("c#") and query parameters?!?
            var queryStringPairs = HttpContext.Current.Request.QueryString.ToPairs();
            var type = QueryStringOrDefaultEnum(queryStringPairs, "type", QueryType.ViewCount);
            var pageSize = QueryStringOrDefaultInt(queryStringPairs, "pageSize", 50);
            var skip = QueryStringOrDefaultInt(queryStringPairs, "skip", 0);
            var otherTag = QueryStringOrDefaultString(queryStringPairs, "otherTag", "");
            var @operator = QueryStringOrDefaultString(queryStringPairs, "operator", "NOT");

            var timer = Stopwatch.StartNew();
            var results = WebApiApplication.TagServer.Value.ComparisonQuery(type, tag, otherTag, @operator, pageSize, skip);
            timer.Stop();

            return new
            {
                Statistics = new
                {
                    Elapsed = timer.Elapsed,
                    ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                    Count = results.Count,
                    TotalCount = WebApiApplication.TagServer.Value.TotalCount(type, tag)
                },
                DEBUGGING = new
                {
                    Tag = tag,
                    QueryType = type.ToString(),
                    PageSize = pageSize,
                    Skip = skip,
                    HttpContext.Current.Request.Path,
                    HttpContext.Current.Request.RawUrl,
                    QueryString = HttpContext.Current.Request.QueryString
                                            .ToPairs()
                                            .ToDictionary(p => p.Key, p => p.Value)
                },
                Results = results,
            };
        }

        private T QueryStringOrDefaultEnum<T>(IEnumerable<KeyValuePair<string, string>> parameters, string name, T defaultValue)
        {
            if (parameters.Any(p => p.Key.ToLowerInvariant() == name.ToLowerInvariant()))
            {
                var match = parameters.First(p => p.Key.ToLowerInvariant() == name.ToLowerInvariant());
                return (T)Enum.Parse(typeof(T), match.Value, ignoreCase: true);
            }
            return defaultValue;
        }

        private int QueryStringOrDefaultInt(IEnumerable<KeyValuePair<string, string>> parameters, string name, int defaultValue)
        {
            if (parameters.Any(p => p.Key.ToLowerInvariant() == name.ToLowerInvariant()))
            {
                var match = parameters.First(p => p.Key.ToLowerInvariant() == name.ToLowerInvariant());
                return int.Parse(match.Value, NumberStyles.Integer);
            }
            return defaultValue;
        }

        private string QueryStringOrDefaultString(IEnumerable<KeyValuePair<string, string>> parameters, string name, string defaultValue)
        {
            if (parameters.Any(p => p.Key.ToLowerInvariant() == name.ToLowerInvariant()))
            {
                var match = parameters.First(p => p.Key.ToLowerInvariant() == name.ToLowerInvariant());
                return match.Value;
            }
            return defaultValue;
        }
    }
}
