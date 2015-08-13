using Ewah;
using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;
using TagByQueryBitMapLookup = System.Collections.Generic.Dictionary<string, Ewah.EwahCompressedBitArray>;
using TagLookup = System.Collections.Generic.Dictionary<string, int>;

namespace StackOverflowTagServer.Querying
{
    internal class BitMapQueryProcessor : BaseQueryProcessor
    {
        private readonly Func<QueryType, TagByQueryBitMapLookup> GetTagByQueryBitMapLookup;
        private readonly TagLookup allTags;

        internal BitMapQueryProcessor(List<Question> questions,
                                      TagLookup allTags,
                                      Func<QueryType, TagByQueryLookup> getQueryTypeInfo,
                                      Func<QueryType, TagByQueryBitMapLookup> getTagByQueryBitMapLookup)
            : base(questions, getQueryTypeInfo)
        {
            GetTagByQueryBitMapLookup = getTagByQueryBitMapLookup;
            this.allTags = allTags;
        }

        internal QueryResult Query(QueryInfo info, EwahCompressedBitArray exclusionBitMap)
        {
            var fieldFetcher = queryTypeLookup[info.Type];
            var bitMap = GetTagByQueryBitMapLookup(info.Type);
            var questionLookup = GetTagByQueryLookup(info.Type)[TagServer.ALL_TAGS_KEY];

            Logger.Log("Tag \"{0}\" is in {1:N0} Questions, Tag \"{2}\" is in {3:N0} Questions", info.Tag, allTags[info.Tag], info.OtherTag, allTags[info.OtherTag]);

            //PrintResults(Enumerable.Range(0, questionLookup.Length), questionLookup, ALL_TAGS_KEY, queryType, fieldFetcher);
            //PrintResults(bitMap[tag1], questionLookup, tag1, queryType, fieldFetcher);
            //PrintResults(bitMap[tag2], questionLookup, tag2, queryType, fieldFetcher);

            var timer = Stopwatch.StartNew();
            var tag1BitMap = bitMap[info.Tag];
            var tag2BitMap = bitMap[info.OtherTag];
            EwahCompressedBitArray bitMapResult = new EwahCompressedBitArray();

            switch (info.Operator)
            {
                case "AND":
                    bitMapResult = tag1BitMap.And(tag2BitMap);
                    break;
                case "AND-NOT":
                    bitMapResult = tag1BitMap.AndNot(tag2BitMap);
                    break;
                case "OR":
                    bitMapResult = tag1BitMap.Or(tag2BitMap);
                    break;
                case "OR-NOT": //"i.e. .net+or+jquery-"
                    // TODO see if it's possible to write a custom OrNot() function (could use AndNot() as a starting point?)
                    var cloneTimer = Stopwatch.StartNew();
                    var notTag2BitMap = (EwahCompressedBitArray)tag2BitMap.Clone();
                    cloneTimer.Stop();

                    var notTimer = Stopwatch.StartNew();
                    notTag2BitMap.Not();
                    notTimer.Stop();

                    var orTimer = Stopwatch.StartNew();
                    bitMapResult = tag1BitMap.Or(notTag2BitMap);
                    orTimer.Stop();

                    Logger.Log("CLONING took {0:N2} ms, NOT took {1:N2} ms, OR took {2:N2} ms",
                               cloneTimer.Elapsed.TotalMilliseconds, notTimer.Elapsed.TotalMilliseconds, orTimer.Elapsed.TotalMilliseconds);
                    break;

                // TODO Work out what this really means, the LINQ version is "result = tag1Query.Except(tag2Query)"
                //case "NOT":
                //    var bitMapResult = (EwahCompressedBitArray)tag2BitMap.Clone();
                //    bitMapResult.Not();
                //    break;

                default:
                    throw new InvalidOperationException(string.Format("Invalid operator specified: {0}", info.Operator ?? "<NULL>"));
            }

            if (exclusionBitMap != null)
            {
                var exclusionTimer = Stopwatch.StartNew();
                bitMapResult = bitMapResult.And(exclusionBitMap);
                exclusionTimer.Stop();
                Logger.Log("Took {0,5:N2} ms to apply the exclusion Bit Map Index (Cardinality={1:N0})",
                           exclusionTimer.Elapsed.TotalMilliseconds, exclusionBitMap.GetCardinality());
            }

            var result = bitMapResult.Skip(info.Skip)
                                     .Take(info.PageSize)
                                     .Select(i => questions[questionLookup[i]])
                                     .ToList();

            timer.Stop();

            using (Utils.SetConsoleColour(ConsoleColor.DarkYellow))
                Logger.Log("Took {0,5:N2} ms in TOTAL to calculate \"{1} {2} {3}\" (Result Cardinality={4:N0})",
                           timer.Elapsed.TotalMilliseconds, info.Tag, info.Operator, info.OtherTag, bitMapResult.GetCardinality());
            //PrintResults(bitMapResult, questionLookup, string.Format("{0} {1} {2)", tag1, @operator, tag2), queryType, fieldFetcher);
            Logger.Log();

            return new QueryResult
            {
                Questions = result,
                //Counters = new Dictionary<string, int>
                //{
                //    { "TagCounter", tagCounter },
                //    { "OtherTagCounter", otherTagCounter },
                //    { "ExclusionCounter", exclusionCounter.Counter }
                //}
            };
        }

        private static Dictionary<QueryType, Func<Question, string>> queryTypeLookup =
            new Dictionary<QueryType, Func<Question, string>>
                {
                    { QueryType.AnswerCount, qu => qu.AnswerCount.HasValue ? qu.AnswerCount.Value.ToString("N0") : "<NULL>" },
                    { QueryType.CreationDate, qu => qu.CreationDate.ToString() },
                    { QueryType.LastActivityDate, qu => qu.LastActivityDate.ToString() },
                    { QueryType.Score, qu => qu.Score.HasValue ? qu.Score.Value.ToString("N0") : "<NULL>" },
                    { QueryType.ViewCount, qu => qu.ViewCount.HasValue ? qu.ViewCount.Value.ToString("N0") : "<NULL>" }
                };

        private void PrintResults(IEnumerable<int> bits, int[] questionLookup, string info, QueryType queryType, Func<Question, string> fieldFetcher)
        {
            Logger.Log("RESULTS for \"{0}\":", info);
            foreach (var bit in bits.Take(10))
            {
                var questionId = questionLookup[bit];
                var question = questions[questionId];
                Logger.Log("  Bit=[{0,9:N0}] -> Qu=[{1,9:N0}], {2}={3,10:N0}, Tags= {4}",
                           bit, questionId, queryType, fieldFetcher(question), String.Join(", ", question.Tags));
            }
        }
    }
}
