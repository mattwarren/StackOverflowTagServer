using Server.Infrastructure;
using StackOverflowTagServer;
using StackOverflowTagServer.DataStructures;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Web;
using System.Web.Http;

namespace Server.Controllers
{
    public class QueryController : ApiController
    {
        [Route("api/Query/{tag}")]
        [HttpGet]
        public object Query(string tag)
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
    }
}
