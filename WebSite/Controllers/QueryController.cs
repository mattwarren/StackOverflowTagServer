using Server.Infrastructure;
using StackOverflowTagServer;
using StackOverflowTagServer.DataStructures;
using System.Diagnostics;
using System.Linq;
using System.Web;
using System.Web.Http;
using Shared;
using System;
using System.Collections.Generic;

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
            var @operator = QueryStringProcessor.GetString(queryStringPairs, "operator", "AND");
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
                    ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                    Count = result.Questions.Count,
                    TotalQuestionsForTag = WebApiApplication.TagServer.Value.TotalCount(type, tag),
                    Counters = result.Counters
                },
                DEBUGGING = new {
                    Tag = tag,
                    QueryType = type.ToString(),
                    PageSize = pageSize,
                    Skip = skip,
                    HttpContext.Current.Request.Path,
                    HttpContext.Current.Request.RawUrl,
                    QueryString = HttpContext.Current.Request.QueryString
                                            .ToPairs()
                                            .ToDictionary(p => p.Key, p => p.Value),
                    TagsBeforeExpansion = leppieWildcards.Count,
                    TagsAfterExpansion = leppieExpandedTags != null ? leppieExpandedTags.Count : 0,
                    TagsExpansionMilliseconds = tagExpansionTimer.Elapsed.TotalMilliseconds.ToString("N2"),
                    QuestionIds = result.Questions.Select(qu => qu.Id),
                    InvalidResults = GetInvalidResults(result.Questions, tag, otherTag, type, @operator),
                    ShouldHaveBeenExcludedResults = GetShouldHaveBeenExcludedResults(result.Questions, type, @operator, leppieExpandedTags)
                },
                Results = result.Questions,
            };
        }

        private List<Question> GetInvalidResults(List<Question> results, string tag, string otherTag, QueryType type, string @operator)
        {
            var invalidResults = new List<Question>();
            switch (@operator)
            {
                case "AND":
                    var andMatches = results.Where(q => q.Tags.Any(t => t == tag && t == otherTag));
                    invalidResults.AddRange(results.Except(andMatches));
                    break;
                case "AND-NOT":
                    var andNotMatches = results.Where(q => q.Tags.Any(t => t == tag && t != otherTag));
                    invalidResults.AddRange(results.Except(andNotMatches));
                    break;
                case "OR":
                    var orMatches = results.Where(q => q.Tags.Any(t => t == tag || t == otherTag));
                    invalidResults.AddRange(results.Except(orMatches));
                    break;
                case "OR-NOT":
                    var orNotMatches = results.Where(q => q.Tags.Any(t => t == tag || t != otherTag));
                    invalidResults.AddRange(results.Except(orNotMatches));
                    break;
                case "NOT":
                    var notMatches = results.Where(q => q.Tags.Any(t => t == tag && t != otherTag));
                    invalidResults.AddRange(results.Except(notMatches));
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Invalid operator specified: {0}", @operator ?? "<NULL>"));
            }
            return invalidResults;
        }

        private List<Question> GetShouldHaveBeenExcludedResults(List<Question> results, QueryType type, string @operator,
                                                                StackOverflowTagServer.CLR.HashSet<string> tagsToExclude)
        {
            if (tagsToExclude == null)
                return new List<Question>();

            return results.Where(q => q.Tags.Any(t => tagsToExclude.Contains(t))).ToList();
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
