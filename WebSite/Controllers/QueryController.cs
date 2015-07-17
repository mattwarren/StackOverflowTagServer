using Server.Infrastructure;
using StackOverflowTagServer;
using StackOverflowTagServer.DataStructures;
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

            // This timer must include everything!! (i.e processing the wildcards and doing the query!!)
            var timer = Stopwatch.StartNew();

            var leppieWildcards = WebApiApplication.LeppieWildcards.Value;
            StackOverflowTagServer.CLR.HashSet<string> leppieExpandedTags = null;
            var tagExpansionTimer = new Stopwatch();
            if (useLeppieExclusions)
            {
                var allTags = WebApiApplication.TagServer.Value.AllTags;
                var nGrams = WebApiApplication.NGrams.Value;
                tagExpansionTimer = Stopwatch.StartNew();
                leppieExpandedTags = WildcardProcessor.ExpandTagsNGrams(allTags, leppieWildcards, nGrams);
                tagExpansionTimer.Stop();
            }

            QueryResult result;
            if (useLinq)
                result = WebApiApplication.TagServer.Value.ComparisonQuery(type, tag, otherTag, @operator, pageSize, skip, tagsToExclude: leppieExpandedTags);
            else
                result = WebApiApplication.TagServer.Value.ComparisonQueryNoLINQ(type, tag, otherTag, @operator, pageSize, skip, tagsToExclude: leppieExpandedTags);

            timer.Stop();

            return new
            {
                Statistics = new {
                    //Elapsed = timer.Elapsed,
                    ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                    Count = result.Questions.Count,
                    NumberOfQuestionsVisited = result.Tag1QueryCounter + result.Tag2QueryCounter,
                    TotalQuestionsForTag = WebApiApplication.TagServer.Value.TotalCount(type, tag)
                },
                DEBUGGING = new {
                    Tag = tag,
                    QueryType = type.ToString(),
                    PageSize = pageSize,
                    Skip = skip,
                    HttpContext.Current.Request.Path,
                    HttpContext.Current.Request.RawUrl,
                    //QueryString = HttpContext.Current.Request.QueryString
                    //                        .ToPairs()
                    //                        .ToDictionary(p => p.Key, p => p.Value),
                    TagsBeforeExpansion = leppieWildcards.Count,
                    TagsAfterExpansion = leppieExpandedTags != null ? leppieExpandedTags.Count : 0,
                    //TagsExpansionElapsed = tagExpansionTimer.Elapsed,
                    TagsExpansionMilliseconds = tagExpansionTimer.Elapsed.TotalMilliseconds.ToString("N2"),
                    QuestionIds = result.Questions.Select(qu => qu.Id)
                },
                Results = result.Questions,
            };
        }

        [Route("api/Query/LeppieWildcards")]
        [HttpGet]
        public object LeppieWildcards()
        {
            var justWildcards = WebApiApplication.LeppieWildcards.Value
                                    .Where(w => w.Contains("*"))
                                    .OrderBy(t => t)
                                    .ToList();
            return new
            {
                Count = WebApiApplication.LeppieWildcards.Value.Count,
                JustWildcardsCount = justWildcards.Count,
                JustWildcards = justWildcards,
                FullList = WebApiApplication.LeppieWildcards.Value.OrderBy(t => t)
            };
        }

        [Route("api/Query/LeppieTags")]
        [HttpGet]
        public object LeppieTags()
        {
            var allTags = WebApiApplication.TagServer.Value.AllTags;
            var leppieWildcards = WebApiApplication.LeppieWildcards.Value;
            var nGrams = WebApiApplication.NGrams.Value;
            var expandedWildcards = WildcardProcessor.ExpandTagsNGrams(allTags, leppieWildcards, nGrams)
                                                    .OrderBy(t => t)
                                                    .ToList();
            return new
            {
                Count = leppieWildcards.Count,
                ExpandedCount = expandedWildcards.Count,
                ExpandedWildcards = expandedWildcards
            };
        }
    }
}
