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

        internal QueryResult Query(QueryInfo info, EwahCompressedBitArray exclusionBitMap = null, bool printLoggingMessages = false)
        {
            var bitMap = GetTagByQueryBitMapLookup(info.Type);
            var questionLookup = GetTagByQueryLookup(info.Type)[TagServer.ALL_TAGS_KEY];

            if (printLoggingMessages)
                Logger.Log("Tag \"{0}\" is in {1:N0} Questions, Tag \"{2}\" is in {3:N0} Questions",
                           info.Tag, allTags[info.Tag], info.OtherTag, allTags[info.OtherTag]);

            //PrintResults(Enumerable.Range(0, questionLookup.Length), qu => questionLookup[qu], TagServer.ALL_TAGS_KEY, info.Type);
            //PrintResults(bitMap[info.Tag], qu => questionLookup[qu], info.Tag, info.Type);
            //PrintResults(bitMap[info.OtherTag], qu => questionLookup[qu], info.OtherTag, info.Type);

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
                    bitMapResult = tag1BitMap.OrNot(tag2BitMap);
                    break;

                // TODO Work out what a "NOT" query really means, the LINQ version was "result = tag1Query.Except(tag2Query)" (which is the same as AND-NOT?!)
                //case "NOT":
                //    var bitMapResult = (EwahCompressedBitArray)tag2BitMap.Clone();
                //    bitMapResult.Not();
                //    break;

                default:
                    throw new InvalidOperationException(string.Format("Invalid operator specified: {0}", info.Operator ?? "<NULL>"));
            }

            if (exclusionBitMap != null)
            {
                ulong cardinalityBeforeExclusions = 0;
                if (printLoggingMessages)
                    cardinalityBeforeExclusions = bitMapResult.GetCardinality();

                var exclusionTimer = Stopwatch.StartNew();
                bitMapResult = bitMapResult.And(exclusionBitMap);
                //bitMapResult = bitMapResult.AndNot(exclusionBitMap);
                exclusionTimer.Stop();

                if (printLoggingMessages)
                {
                    Logger.Log("Took {0,5:N2} ms to apply exclusion BitMap (Cardinality={1:N0}), Results Cardinality: Before={2:N0}, After={3:N0}",
                               exclusionTimer.Elapsed.TotalMilliseconds, exclusionBitMap.GetCardinality(), cardinalityBeforeExclusions, bitMapResult.GetCardinality());
                    //Logger.Log("Took {0,5:N2} ms to apply exclusion BitMap", exclusionTimer.Elapsed.TotalMilliseconds);
                }
            }

            // TODO work out if we can improve the speed of this, can take almsot 100 ms?!
            var resultCollectionTimer = Stopwatch.StartNew();
            var result = bitMapResult.Skip(info.Skip)
                                     .Take(info.PageSize)
                                     .Select(i => questions[questionLookup[i]])
                                     .ToList();
            resultCollectionTimer.Stop();
            if (printLoggingMessages)
                Logger.Log("Took {0,5:N2} ms to collect the results", resultCollectionTimer.Elapsed.TotalMilliseconds);

            timer.Stop();

            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));

            if (printLoggingMessages)
            {
                using (Utils.SetConsoleColour(ConsoleColor.DarkYellow))
                {
                    //Logger.Log("Took {0,5:N2} ms in TOTAL to calculate \"{1} {2} {3}\" (Result Cardinality={4:N0})",
                    //           timer.Elapsed.TotalMilliseconds, info.Tag, info.Operator, info.OtherTag, bitMapResult.GetCardinality());
                    Logger.Log("Took {0,5:N2} ms in TOTAL to calculate \"{1} {2} {3}\"",
                               timer.Elapsed.TotalMilliseconds, info.Tag, info.Operator, info.OtherTag);
                }
                //PrintResults(bitMapResult, qu => questionLookup[qu], string.Format("{0} {1} {2}", info.Tag, info.Operator, info.OtherTag), info.Type);
                Logger.Log();
            }

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
    }
}
