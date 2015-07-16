using Microsoft.ApplicationInsights;
using Server.Infrastructure;
using StackOverflowTagServer;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Web;
using System.Web.Http;

namespace Server.Controllers
{
    [ActionWebApiFilter]
    public class TagsController : ApiController
    {
        // GET: api/Tags/Get
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

        // GET: api/Tags/Get/.net, api/Tags/Get/c%23, api/Tags/Get/java,
        public object Get(string tag)
        {
            // Couldn't get Web API to play nice with "c%23" ("c#") and query parameters?!?
            var queryStringPairs = HttpContext.Current.Request.QueryString.ToPairs();
            var type = QueryStringProcessor.GetEnum(queryStringPairs, "type", QueryType.ViewCount);
            var pageSize = QueryStringProcessor.GetInt(queryStringPairs, "pageSize", 50);
            var skip = QueryStringProcessor.GetInt(queryStringPairs, "skip", 0);

            var timer = Stopwatch.StartNew();
            var results = WebApiApplication.TagServer.Value.Query(type, tag, pageSize, skip);
            timer.Stop();

            return new
            {
                Statistics = new {
                    Elapsed = timer.Elapsed,
                    ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                    Count = results.Count,
                    TotalQuestionsForTag = WebApiApplication.TagServer.Value.TotalCount(type, tag)
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
            var type = QueryStringProcessor.GetEnum(queryStringPairs, "type", QueryType.ViewCount);
            var pageSize = QueryStringProcessor.GetInt(queryStringPairs, "pageSize", 50);
            var skip = QueryStringProcessor.GetInt(queryStringPairs, "skip", 0);
            var otherTag = QueryStringProcessor.GetString(queryStringPairs, "otherTag", "");
            var @operator = QueryStringProcessor.GetString(queryStringPairs, "operator", "NOT");
            var useLinq = QueryStringProcessor.GetBool(queryStringPairs, "useLinq", false);
            var useLeppieExclusions = QueryStringProcessor.GetBool(queryStringPairs, "leppieExclusions", false);

            var allTags = WebApiApplication.TagServer.Value.AllTags;
            var leppieTags = WebApiApplication.LeppieTags.Value;
            var nGrams = WebApiApplication.NGrams.Value;
            var tagExpansionTimer = Stopwatch.StartNew();
            var leppieExpandedTags = WildcardProcessor.ExpandTagsNGrams(allTags, leppieTags, nGrams);
            tagExpansionTimer.Stop();

            List<Shared.Question> results;
            var timer = Stopwatch.StartNew();
            if (useLinq)
                results = WebApiApplication.TagServer.Value.ComparisonQuery(type, tag, otherTag, @operator, pageSize, skip);
            else
                results = WebApiApplication.TagServer.Value.ComparisonQueryNoLINQ(type, tag, otherTag, @operator, pageSize, skip);
            timer.Stop();

            return new
            {
                Statistics = new
                {
                    Elapsed = timer.Elapsed,
                    ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                    Count = results.Count,
                    TotalQuestionsForTag = WebApiApplication.TagServer.Value.TotalCount(type, tag)
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
                                            .ToDictionary(p => p.Key, p => p.Value),
                    TagsBeforeExpansion = leppieTags.Count,
                    TagsAfterExpansion = leppieExpandedTags.Count,
                    TagsExpansionElapsed = tagExpansionTimer.Elapsed,
                    TagsExpansionMilliseconds = tagExpansionTimer.Elapsed.TotalMilliseconds.ToString("N2"),
                },
                Results = results,
            };
        }

        // GET: api/Tags/Question, e.g. api/Tags/Question?id=472906 (api/Tags/Question/472906 DOESN'T work!!)
        [HttpGet]
        public Shared.Question Question(int id)
        {
            return WebApiApplication.TagServer.Value.Questions.Where(qu => qu.Id == id).FirstOrDefault();
        }

        private object GetAPIInfo(TagServer tagServer)
        {
            var urlRoot = "http://" + Request.RequestUri.Host + ":" + Request.RequestUri.Port;
            return new
            {
                SampleUrls = new
                {
                    BasicQueries = new Dictionary<string, string>
                        {
                            { Request.RequestUri.ToString() + "/c%23",
                                "c# questions (have to escape \"c#\" -> \"c%23\"), defaults to QueryType.ViewCount, pageSize = 50, skip = 0" },
                            { Request.RequestUri.ToString() + "/c%23?Type=LastActivityDate&PageSize=25",
                                "25 Most Recent c# questions" },
                            { Request.RequestUri.ToString() + "/c%23?Type=Score&PageSize=1&Skip=71993",
                                "Lowest scoring c# question" },
                            { Request.RequestUri.ToString() + "/.net?Type=AnswerCount&PageSize=1",
                                "The .NET question with the most answers" },
                            {  urlRoot + "/api/Tags/Question?id=472906",
                                "Get an individual question (by Id)" }
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
    }
}
