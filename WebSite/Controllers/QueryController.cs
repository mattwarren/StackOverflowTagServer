using Server.Infrastructure;
using Shared;
using StackOverflowTagServer;
using StackOverflowTagServer.DataStructures;
using StackOverflowTagServer.Querying;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Web;
using System.Web.Http;

using HashSet = StackOverflowTagServer.CLR.HashSet<string>;

namespace Server.Controllers
{
    public class QueryController : ApiController
    {
        [Route("api/Query/{tag}")]
        [HttpGet]
        public object Query(string tag)
        {
            var queryInfo = GetQueryInfo(HttpContext.Current.Request.QueryString.ToPairs(), tag);

            // This timer must include everything!! (i.e processing the wildcards and doing the query!!)
            var timer = Stopwatch.StartNew();

            var leppieWildcards = WebApiApplication.LeppieWildcards.Value;
            HashSet leppieExpandedTags = null;
            var tagExpansionTimer = new Stopwatch();
            if (queryInfo.UseLeppieExclusions)
            {
                var allTags = WebApiApplication.TagServer.Value.AllTags;
                var nGrams = WebApiApplication.NGrams.Value;
                tagExpansionTimer = Stopwatch.StartNew();
                leppieExpandedTags = WildcardProcessor.ExpandTagsNGrams(allTags, leppieWildcards, nGrams);
                tagExpansionTimer.Stop();
            }

            QueryResult result;
            if (queryInfo.UseLinq)
                result = WebApiApplication.TagServer.Value.ComparisonQuery(queryInfo, tagsToExclude: leppieExpandedTags);
            else
                result = WebApiApplication.TagServer.Value.ComparisonQueryNoLINQ(queryInfo, tagsToExclude: leppieExpandedTags);

            timer.Stop();

            var jsonResults = new Dictionary<string, object>();
            jsonResults.Add("Statistics", GetStatistics(queryInfo, result, timer.Elapsed));
            if (queryInfo.DebugMode)
                jsonResults.Add("DEBUGGING", GetDebugInfo(queryInfo, result, timer.Elapsed, leppieWildcards, leppieExpandedTags));
            jsonResults.Add("Results", result.Questions);
            return jsonResults;
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

        [Route("api/Query/LeppieExcludedTags")]
        [HttpGet]
        public object LeppieExpandedWildcards()
        {
            var allTags = WebApiApplication.TagServer.Value.AllTags;
            var leppieWildcards = WebApiApplication.LeppieWildcards.Value;
            var nGrams = WebApiApplication.NGrams.Value;
            var timer = Stopwatch.StartNew();
            var expandedWildcards = WildcardProcessor.ExpandTagsNGrams(allTags, leppieWildcards, nGrams)
                                                     .OrderBy(t => t)
                                                     .ToList();
            timer.Stop();

            return new
            {
                ElapsedMilliseconds = timer.Elapsed.TotalMilliseconds.ToString("N2"),
                CountBeforeExpansion = leppieWildcards.Count,
                ExpandedCount = expandedWildcards.Count,
                ExpandedWildcards = expandedWildcards
            };
        }


        private object GetDebugInfo(QueryInfo queryInfo, QueryResult result, TimeSpan elapsed, List<string> leppieWildcards, HashSet leppieExpandedTags)
        {
            return new
            {
                Tag = queryInfo.Tag,
                OtherTag = queryInfo.OtherTag,
                Operator = queryInfo.Operator,
                QueryType = queryInfo.Type.ToString(),
                PageSize = queryInfo.PageSize,
                Skip = queryInfo.Skip,
                HttpContext.Current.Request.Path,
                HttpContext.Current.Request.RawUrl,
                QueryString = HttpContext.Current.Request.QueryString
                                               .ToPairs()
                                               .ToDictionary(p => p.Key, p => p.Value),
                TagsBeforeExpansion = leppieWildcards.Count,
                TagsAfterExpansion = leppieExpandedTags != null ? leppieExpandedTags.Count : 0,
                TagsExpansionMilliseconds = elapsed.TotalMilliseconds.ToString("N2"),
                QuestionIds = result.Questions.Select(qu => qu.Id),
                InvalidResults = GetInvalidResults(result.Questions, queryInfo.Tag, queryInfo.OtherTag, queryInfo.Type, queryInfo.Operator),
                ShouldHaveBeenExcludedResults = GetShouldHaveBeenExcludedResults(result.Questions, queryInfo.Type, queryInfo.Operator, leppieExpandedTags)
            };
        }

        private object GetStatistics(QueryInfo queryInfo, QueryResult result, TimeSpan elapsed)
        {
            return new
            {
                ElapsedMilliseconds = elapsed.TotalMilliseconds.ToString("N2"),
                ResultCount = result.Questions.Count,
                TotalQuestionsPerTag = new Dictionary<string, string>()
                {
                    { queryInfo.Tag, WebApiApplication.TagServer.Value.TotalCount(queryInfo.Type, queryInfo.Tag).ToString("N0") },
                    { queryInfo.OtherTag, WebApiApplication.TagServer.Value.TotalCount(queryInfo.Type, queryInfo.OtherTag).ToString("N0") }
                },
                Counters = result.Counters
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

        private QueryInfo GetQueryInfo(IEnumerable<KeyValuePair<string, string>> queryStringPairs, string tag)
        {
            return new QueryInfo
            {
                Type = QueryStringProcessor.GetEnum(queryStringPairs, "type", QueryType.ViewCount),
                PageSize = QueryStringProcessor.GetInt(queryStringPairs, "pageSize", 50),
                Skip = QueryStringProcessor.GetInt(queryStringPairs, "skip", 0),

                Tag = tag,
                OtherTag = QueryStringProcessor.GetString(queryStringPairs, "otherTag", ""),
                Operator = QueryStringProcessor.GetString(queryStringPairs, "operator", "AND"),

                UseLinq = QueryStringProcessor.GetBool(queryStringPairs, "useLinq", false),
                UseLeppieExclusions = QueryStringProcessor.GetBool(queryStringPairs, "leppieExclusions", false),
                DebugMode = QueryStringProcessor.GetBool(queryStringPairs, "debugMode", false)
            };
        }
    }
}
