using Ewah;
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

            Stopwatch exclusionBitMapTimer = new Stopwatch();
            EwahCompressedBitArray exclusionBitMap = null;
            if (queryInfo.UseBitMapIndexes && queryInfo.UseLeppieExclusions)
            {
                exclusionBitMapTimer.Start();
                exclusionBitMap = tagServer.CreateBitMapIndexForExcludedTags(leppieExpandedTags, queryInfo.Type);
                exclusionBitMapTimer.Stop();
            }

            QueryResult result;
            var queryTimer = Stopwatch.StartNew();
            if (queryInfo.UseBitMapIndexes)
                result = tagServer.ComparisionQueryBitMapIndex(queryInfo, exclusionBitMap);
            else if (queryInfo.UseLinq)
                result = tagServer.ComparisonQuery(queryInfo, tagsToExclude: leppieExpandedTags);
            else
                result = tagServer.ComparisonQueryNoLINQ(queryInfo, tagsToExclude: leppieExpandedTags);
            queryTimer.Stop();

            // Stop the overall timer, as we don't want to include the time taken to create DEBUG info in it
            timer.Stop();

            var jsonResults = new Dictionary<string, object>();
            jsonResults.Add("Statistics", GetStatistics(queryInfo, result, timer.Elapsed));
            if (queryInfo.DebugMode)
            {
                var debugInfo = GetDebugInfo(queryInfo, result,
                                             leppieWildcards, leppieExpandedTags,
                                             totalTime: timer.Elapsed,
                                             queryTime: queryTimer.Elapsed,
                                             tagsExpansionTime: tagExpansionTimer.Elapsed,
                                             exclusionBitMapTime: exclusionBitMapTimer.Elapsed);
                jsonResults.Add("DEBUGGING", debugInfo);
            }
            jsonResults.Add("Results", result.Questions);
            return jsonResults;
        }

        private object GetDebugInfo(QueryInfo queryInfo, QueryResult result, List<string> leppieWildcards, HashSet leppieExpandedTags,
                                    TimeSpan totalTime, TimeSpan queryTime, TimeSpan tagsExpansionTime, TimeSpan exclusionBitMapTime)
        {
            return new
            {
                Operator = queryInfo.Operator, // In QueryInfo this is just printed as the number, i.e. "3", rather than "ViewCount"
                QueryInfo = queryInfo,
                HttpContext.Current.Request.Path,
                HttpContext.Current.Request.RawUrl,
                //QueryString = HttpContext.Current.Request.QueryString
                //                                 .ToPairs()
                //                                 .ToDictionary(p => p.Key, p => p.Value),
                TimingsInMilliseconds = new
                {
                    TotalTime = totalTime.TotalMilliseconds.ToString("N2"),
                    QueryTime = queryTime.TotalMilliseconds.ToString("N2"),
                    TagsExpansion = tagsExpansionTime.TotalMilliseconds.ToString("N2"),
                    ExclusionBitMap = exclusionBitMapTime.TotalMilliseconds.ToString("N2"),
                    RemainingTime = (totalTime - queryTime - tagsExpansionTime - exclusionBitMapTime).TotalMilliseconds.ToString("N2"),
                },
                TagsBeforeExpansion = leppieWildcards.Count,
                TagsAfterExpansion = leppieExpandedTags != null ? leppieExpandedTags.Count : 0,
                InvalidResults = GetInvalidResults(result.Questions, queryInfo.Tag, queryInfo.OtherTag, queryInfo.Type, queryInfo.Operator),
                ShouldHaveBeenExcludedResults = GetShouldHaveBeenExcludedResults(result.Questions, queryInfo.Type, queryInfo.Operator, leppieExpandedTags),
                //QuestionIds = result.Questions.Select(qu => qu.Id),
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
