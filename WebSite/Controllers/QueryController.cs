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
            // This timer must include everything!! (i.e processing the wildcards and doing the query!!)
            var timer = Stopwatch.StartNew();

            var queryInfo = GetQueryInfo(HttpContext.Current.Request.QueryString.ToPairs(), tag);
            var tagServer = WebApiApplication.TagServer.Value;

            var leppieWildcards = WebApiApplication.LeppieWildcards.Value;
            HashSet leppieExpandedTags = null;
            var tagExpansionTimer = new Stopwatch();
            if (queryInfo.UseLeppieExclusions)
            {
                var allTags = tagServer.AllTags;
                var nGrams = WebApiApplication.NGrams.Value;
                tagExpansionTimer = Stopwatch.StartNew();
                leppieExpandedTags = WildcardProcessor.ExpandTagsNGrams(allTags, leppieWildcards, nGrams);
                tagExpansionTimer.Stop();
            }

            QueryResult result;
            Stopwatch exclusionBitMapTimer = new Stopwatch();
            if (queryInfo.UseBitMapIndexes)
            {
                if (queryInfo.UseLeppieExclusions)
                {
                    exclusionBitMapTimer.Start();
                    var exclusionBitMap = tagServer.CreateBitMapIndexForExcludedTags(leppieExpandedTags, queryInfo.Type);
                    exclusionBitMapTimer.Stop();
                    result = tagServer.ComparisionQueryBitMapIndex(queryInfo, exclusionBitMap);
                }
                else
                {
                    result = tagServer.ComparisionQueryBitMapIndex(queryInfo);
                }
            }
            else if (queryInfo.UseLinq)
                result = tagServer.ComparisonQuery(queryInfo, tagsToExclude: leppieExpandedTags);
            else
                result = tagServer.ComparisonQueryNoLINQ(queryInfo, tagsToExclude: leppieExpandedTags);

            timer.Stop();

            var jsonResults = new Dictionary<string, object>();
            jsonResults.Add("Statistics", GetStatistics(queryInfo, result, timer.Elapsed));
            if (queryInfo.DebugMode)
                jsonResults.Add("DEBUGGING", GetDebugInfo(queryInfo, result, leppieWildcards, leppieExpandedTags,
                                                          tagExpansionTimer.Elapsed, exclusionBitMapTimer.Elapsed, timer.Elapsed));
            jsonResults.Add("Results", result.Questions);
            return jsonResults;
        }

        private object GetDebugInfo(QueryInfo queryInfo, QueryResult result, List<string> leppieWildcards, HashSet leppieExpandedTags,
                                    TimeSpan tagsExpansionTime, TimeSpan exclusionBitMapTime, TimeSpan totalTime)
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
                TimingsInMilliseconds = new
                {
                    TotalTime = totalTime.TotalMilliseconds.ToString("N2"),
                    TagsExpansion = tagsExpansionTime.TotalMilliseconds.ToString("N2"),
                    ExclusionBitMap = exclusionBitMapTime.TotalMilliseconds.ToString("N2"),
                    RemainingTime = (totalTime - tagsExpansionTime - exclusionBitMapTime).TotalMilliseconds.ToString("N2"),
                },
                TagsBeforeExpansion = leppieWildcards.Count,
                TagsAfterExpansion = leppieExpandedTags != null ? leppieExpandedTags.Count : 0,
                InvalidResults = GetInvalidResults(result.Questions, queryInfo.Tag, queryInfo.OtherTag, queryInfo.Type, queryInfo.Operator),
                ShouldHaveBeenExcludedResults = GetShouldHaveBeenExcludedResults(result.Questions, queryInfo.Type, queryInfo.Operator, leppieExpandedTags),
                QuestionIds = result.Questions.Select(qu => qu.Id),
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
                UseBitMapIndexes = QueryStringProcessor.GetBool(queryStringPairs, "bitMapIndex", false),

                UseLeppieExclusions = QueryStringProcessor.GetBool(queryStringPairs, "leppieExclusions", false),
                DebugMode = QueryStringProcessor.GetBool(queryStringPairs, "debugMode", false)
            };
        }
    }
}
