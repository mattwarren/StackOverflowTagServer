using Ewah;
using ProtoBuf;
using Shared;
using StackOverflowTagServer.DataStructures;
using StackOverflowTagServer.Querying;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;
using TagByQueryBitMapLookup = System.Collections.Generic.Dictionary<string, Ewah.EwahCompressedBitArray>;
using TagLookup = System.Collections.Generic.Dictionary<string, int>;

namespace StackOverflowTagServer
{
    public class TagServer
    {
        private readonly TagLookup allTags;
        public TagLookup AllTags { get { return allTags; } }

        private readonly List<Question> questions;
        public List<Question> Questions { get { return questions; } }

        /// <summary> _ALL_TAGS_ </summary>
        public static string ALL_TAGS_KEY = "_ALL_TAGS_";

        private readonly static string AllTagsFileName = "intermediate-AllTags.bin";

        // GetTagLookupForQueryType(QueryType type) maps these Dictionaries to a QueryType (enum)
        private readonly TagByQueryLookup tagsByAnswerCount;
        private readonly TagByQueryLookup tagsByCreationDate;
        private readonly TagByQueryLookup tagsByLastActivityDate;
        private readonly TagByQueryLookup tagsByScore;
        private readonly TagByQueryLookup tagsByViewCount;

        // GetTagBitMapIndexForQueryType(QueryType type) maps these Dictionaries to a QueryType (enum)
        private readonly TagByQueryBitMapLookup tagsByAnswerCountBitMapIndex;
        private readonly TagByQueryBitMapLookup tagsByCreationDateBitMapIndex;
        private readonly TagByQueryBitMapLookup tagsByLastActivityDateBitMapIndex;
        private readonly TagByQueryBitMapLookup tagsByScoreBitMapIndex;
        private readonly TagByQueryBitMapLookup tagsByViewCountBitMapIndex;

        private readonly QueryProcessor queryProcessor;
        private readonly ComplexQueryProcessor complexQueryProcessor;
        private readonly BitMapQueryProcessor bitMapQueryProcessor;
        private readonly BitMapIndexHandler bitMapIndexHandler;
        private readonly Validator validator;

        public static List<Question> GetRawQuestionsFromDisk(string folder, string filename)
        {
            List<Question> rawQuestions;
            var fileReadTimer = Stopwatch.StartNew();
            Logger.LogStartupMessage("DE-serialising the Stack Overflow Questions from the disk....");
            using (var file = File.OpenRead(Path.Combine(folder, filename)))
            {
                rawQuestions = Serializer.Deserialize<List<Question>>(file);
            }
            fileReadTimer.Stop();

            GC.Collect(2, GCCollectionMode.Forced);
            var memoryUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("Took {0} to DE-serialise {1:N0} Stack Overflow Questions from disk - Using {2:N2} MB ({3:N2} GB) of memory\n",
                fileReadTimer.Elapsed, rawQuestions.Count, memoryUsed, memoryUsed / 1024.0);

            return rawQuestions;
        }

        /// <summary>
        /// Factory method to create a <see cref="TagServer"/>, uses the private Constructor <see cref="TagServer(List{Question})" />
        /// </summary>
        public static TagServer CreateFromScratchAndSaveToDisk(List<Question> rawQuestions, string intermediateFilesFolder)
        {
            var tagServer = new TagServer(rawQuestions);
            var serializeTimer = Stopwatch.StartNew();
            Logger.LogStartupMessage("Serialisation folder: {0}", intermediateFilesFolder);

            foreach (QueryType type in (QueryType[])Enum.GetValues(typeof(QueryType)))
            {
                var tagLookupFileName = "intermediate-Lookup-" + type + ".bin";
                Serialisation.SerialiseToDisk(tagLookupFileName, intermediateFilesFolder, tagServer.GetTagLookupForQueryType(type));

                var bitMapIndex = tagServer.GetTagBitMapIndexForQueryType(type);
                if (bitMapIndex.Count == 0)
                    continue;

                var bitMapIndexFileName = String.Format("intermediate-BitMap-{0}.bin", type);
                Serialisation.SerialiseBitMapIndexToDisk(bitMapIndexFileName, intermediateFilesFolder, bitMapIndex);

                // Sanity-check, de-serialise the data we've just written to disk
                Serialisation.DeserialiseFromDisk(bitMapIndexFileName, intermediateFilesFolder);

                Logger.LogStartupMessage();
            }

            // Now write out the AllTags Lookup, Tag -> Count (i.e. "C#" -> 579,321, "Java" -> 560,432)
            Serialisation.SerialiseToDisk(AllTagsFileName, intermediateFilesFolder, tagServer.AllTags);
            serializeTimer.Stop();
            Logger.LogStartupMessage("\nTook {0} (in TOTAL) to serialise the intermediate data TO disk\n", serializeTimer.Elapsed);

            return tagServer;
        }

        /// <summary>
        /// Factory method to create a <see cref="TagServer"/>, uses the private Constructor
        /// <see cref="TagServer(List{Question}, TagLookup, Dictionary{QueryType, TagByQueryLookup}, Dictionary{QueryType, TagByQueryBitMapLookup})"/>
        /// </summary>
        public static TagServer CreateFromSerialisedData(List<Question> rawQuestions, string intermediateFilesFolder, bool deserialiseBitMapsIndexes = true)
        {
            var deserializeTimer = Stopwatch.StartNew();
            Logger.LogStartupMessage("Deserialisation folder: {0}", intermediateFilesFolder);
            var queryTypes = (QueryType[])Enum.GetValues(typeof(QueryType));
            var intermediateLookups = new Dictionary<QueryType, TagByQueryLookup>(queryTypes.Length);
            var intermediateBitMapIndexes = new Dictionary<QueryType, TagByQueryBitMapLookup>(queryTypes.Length);
            foreach (QueryType type in queryTypes)
            {
                if (deserialiseBitMapsIndexes)
                {
                    var tagLookupFileName = "intermediate-Lookup-" + type + ".bin";
                    var tempLookup = Serialisation.DeserialiseFromDisk<TagByQueryLookup>(tagLookupFileName, intermediateFilesFolder);
                    Logger.LogStartupMessage("{0,20} contains {1:N0} Tag Lookups", type, tempLookup.Count);
                    intermediateLookups.Add(type, tempLookup);

                    var bitMapIndexFileName = String.Format("intermediate-BitMap-{0}.bin", type);
                    var tempBitMapIndexes = Serialisation.DeserialiseFromDisk(bitMapIndexFileName, intermediateFilesFolder);
                    Logger.LogStartupMessage("{0,20} contains {1:N0} Tag BitMap Indexes", type, tempBitMapIndexes.Count);
                    intermediateBitMapIndexes.Add(type, tempBitMapIndexes);

                    Logger.LogStartupMessage();
                }
                else
                {
                    // Maybe don't check this in, it's just here to save time when we don't need to real data!!!
                    var tempLookup = new TagByQueryLookup();
                    intermediateLookups.Add(type, tempLookup);
                    var tempBitMapIndexes = new TagByQueryBitMapLookup();
                    intermediateBitMapIndexes.Add(type, tempBitMapIndexes);
                }
            }

            // Now fetch from disk the AllTags Lookup, Tag -> Count (i.e. "C#" -> 579,321, "Java" -> 560,432)
            var allTags = Serialisation.DeserialiseFromDisk<TagLookup>(AllTagsFileName, intermediateFilesFolder);
            deserializeTimer.Stop();
            Logger.LogStartupMessage("\nTook {0} (in TOTAL) to DE-serialise the intermediate data FROM disk\n", deserializeTimer.Elapsed);

            return new TagServer(rawQuestions, allTags, intermediateLookups, intermediateBitMapIndexes);
        }

        public int TotalCount(QueryType type, string tag)
        {
            TagByQueryLookup queryInfo = GetTagLookupForQueryType(type);
            return queryInfo[tag].Length;
        }

        /// <summary>
        /// Private constructor that is used when creating the Tag Server from SCRATCH (<see cref="CreateFromScratchAndSaveToDisk"/>)
        /// </summary>
        /// <param name="questionsList"></param>
        private TagServer(List<Question> questionsList)
        {
            questions = questionsList;
            var groupedTags = CreateTagGroupings();
            allTags = groupedTags.ToDictionary(t => t.Key, t => t.Value.Count);

            // Some of these rely on allTags, so they have to be created AFTER it is
            queryProcessor = new QueryProcessor(questions, type => GetTagLookupForQueryType(type));
            complexQueryProcessor = new ComplexQueryProcessor(questions, type => GetTagLookupForQueryType(type));
            bitMapQueryProcessor = new BitMapQueryProcessor(questions, allTags,
                                                            type => GetTagLookupForQueryType(type),
                                                            type => GetTagBitMapIndexForQueryType(type));
            bitMapIndexHandler = new BitMapIndexHandler(questions, allTags,
                                                        type => GetTagLookupForQueryType(type),
                                                        type => GetTagBitMapIndexForQueryType(type));
            validator = new Validator(questions, allTags,
                                      type => GetTagLookupForQueryType(type),
                                      type => GetTagBitMapIndexForQueryType(type));

            // These have to be initialised in the ctor, so they can remain readonly
            tagsByAnswerCount = new TagByQueryLookup(groupedTags.Count);
            tagsByCreationDate = new TagByQueryLookup(groupedTags.Count);
            tagsByLastActivityDate = new TagByQueryLookup(groupedTags.Count);
            tagsByScore = new TagByQueryLookup(groupedTags.Count);
            tagsByViewCount = new TagByQueryLookup(groupedTags.Count);

            // These have to be initialised in the ctor, so they can remain readonly
            tagsByAnswerCountBitMapIndex = new TagByQueryBitMapLookup(groupedTags.Count);
            tagsByCreationDateBitMapIndex = new TagByQueryBitMapLookup(groupedTags.Count);
            tagsByLastActivityDateBitMapIndex = new TagByQueryBitMapLookup(groupedTags.Count);
            tagsByScoreBitMapIndex = new TagByQueryBitMapLookup(groupedTags.Count);
            tagsByViewCountBitMapIndex = new TagByQueryBitMapLookup(groupedTags.Count);

            CreateSortedLists(groupedTags, useAlternativeMethod: true);

            Logger.LogStartupMessage(new string('#', Console.WindowWidth));
            bitMapIndexHandler.CreateBitMapIndexes();
            Logger.LogStartupMessage(new string('#', Console.WindowWidth));

            validator.ValidateTagOrdering();
            validator.ValidateBitMapIndexOrdering();

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("After TagServer created - Using {0:N2} MB ({1:N2} GB) of memory in total\n", mbUsed, mbUsed / 1024.0);
        }

        /// <summary>
        /// Private constructor that is used when creating the Tag Server from previously serialised data (<see cref="CreateFromSerialisedData"/>)
        /// </summary>
        private TagServer(List<Question> questionsList, TagLookup allTags,
                          Dictionary<QueryType, TagByQueryLookup> intermediateLookups,
                          Dictionary<QueryType, TagByQueryBitMapLookup> intermediateBitMapIndexes)
        {
            questions = questionsList;
            this.allTags = allTags;

            // Some of these rely on allTags, so they have to be created AFTER it is
            queryProcessor = new QueryProcessor(questions, type => GetTagLookupForQueryType(type));
            complexQueryProcessor = new ComplexQueryProcessor(questions, type => GetTagLookupForQueryType(type));
            bitMapQueryProcessor = new BitMapQueryProcessor(questions, allTags,
                                                            type => GetTagLookupForQueryType(type),
                                                            type => GetTagBitMapIndexForQueryType(type));
            bitMapIndexHandler = new BitMapIndexHandler(questions, allTags,
                                                        type => GetTagLookupForQueryType(type),
                                                        type => GetTagBitMapIndexForQueryType(type));
            validator = new Validator(questions, allTags,
                                      type => GetTagLookupForQueryType(type),
                                      type => GetTagBitMapIndexForQueryType(type));

            // These have to be initialised in the ctor, so they can remain readonly
            tagsByAnswerCount = intermediateLookups[QueryType.AnswerCount];
            tagsByCreationDate = intermediateLookups[QueryType.CreationDate];
            tagsByLastActivityDate = intermediateLookups[QueryType.LastActivityDate];
            tagsByScore = intermediateLookups[QueryType.Score];
            tagsByViewCount = intermediateLookups[QueryType.ViewCount];

            // These have to be initialised in the ctor, so they can remain readonly
            tagsByAnswerCountBitMapIndex = intermediateBitMapIndexes[QueryType.AnswerCount];
            tagsByCreationDateBitMapIndex = intermediateBitMapIndexes[QueryType.CreationDate];
            tagsByLastActivityDateBitMapIndex = intermediateBitMapIndexes[QueryType.LastActivityDate];
            tagsByScoreBitMapIndex = intermediateBitMapIndexes[QueryType.Score];
            tagsByViewCountBitMapIndex = intermediateBitMapIndexes[QueryType.ViewCount];

            // This takes a while, maybe don't do it when using Intermediate results (that have already had this check done when they were created)
            //ValidateTagOrdering();
            //ValidateBitMapIndexOrdering();

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("After TagServer created - Using {0:N2} MB ({1:N2} GB) of memory in total\n", mbUsed, mbUsed / 1024.0);
        }

#region PublicApiPassedThruToRelevantClass

        public List<Question> Query(QueryType type, string tag, int pageSize = 50, int skip = 0 /*, bool ascending = true*/)
        {
            return queryProcessor.Query(type, tag, pageSize, skip);
        }

        public QueryResult ComparisonQuery(QueryInfo info, CLR.HashSet<string> tagsToExclude = null)
        {
            return complexQueryProcessor.Query(info, tagsToExclude);
        }

        public QueryResult ComparisonQueryNoLINQ(QueryInfo info, CLR.HashSet<string> tagsToExclude = null)
        {
            return complexQueryProcessor.QueryNoLINQ(info, tagsToExclude);
        }

        public QueryResult ComparisionQueryBitMapIndex(QueryInfo info, EwahCompressedBitArray exclusionBitMap = null, bool printLoggingMessages = false)
        {
            return bitMapQueryProcessor.Query(info, exclusionBitMap, printLoggingMessages);
        }

        public EwahCompressedBitArray CreateBitMapIndexForExcludedTags(CLR.HashSet<string> tagsToExclude, QueryType queryType, bool printLoggingMessages = false)
        {
            return bitMapIndexHandler.CreateBitMapIndexForExcludedTags(tagsToExclude, queryType, printLoggingMessages);
        }

        public List<Question> GetInvalidResults(List<Question> results, QueryInfo queryInfo)
        {
            return validator.GetInvalidResults(results, queryInfo);
        }

        public List<Tuple<Question, List<string>>> GetShouldHaveBeenExcludedResults(List<Question> results, QueryInfo queryInfo, CLR.HashSet<string> tagsToExclude)
        {
            return validator.GetShouldHaveBeenExcludedResults(results, queryInfo, tagsToExclude);
        }

        internal void ValidateExclusionBitMap(EwahCompressedBitArray bitMapIndex, CLR.HashSet<string> expandedTagsNGrams, QueryType queryType)
        {
            validator.ValidateExclusionBitMap(bitMapIndex, expandedTagsNGrams, queryType);
        }

        internal List<Question> BooleanQueryWithExclusionsLINQVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.BooleanQueryWithExclusionsLINQVersion(type, tag, excludedTags, pageSize, skip);
        }

        internal List<Question> BooleanQueryWithExclusionsFastVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.BooleanQueryWithExclusionsFastVersion(type, tag, excludedTags, pageSize, skip);
        }

        internal List<Question> BooleanQueryWithExclusionsFastAlternativeVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.BooleanQueryWithExclusionsFastAlternativeVersion(type, tag, excludedTags, pageSize, skip);
        }

        internal List<Question> BooleanQueryWithExclusionsBloomFilterVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.BooleanQueryWithExclusionsBloomFilterVersion(type, tag, excludedTags, pageSize, skip);
        }

#endregion PublicApiPassedThruToRelevantClass

        private TagByQueryLookup GetTagLookupForQueryType(QueryType type)
        {
            switch (type)
            {
                case QueryType.LastActivityDate:
                    return tagsByLastActivityDate;
                case QueryType.CreationDate:
                    return tagsByCreationDate;
                case QueryType.Score:
                    return tagsByScore;
                case QueryType.ViewCount:
                    return tagsByViewCount;
                case QueryType.AnswerCount:
                    return tagsByAnswerCount;
                default:
                    throw new InvalidOperationException(string.Format("GetTagLookupForQueryType - Invalid query type {0}", (int)type));
            }
        }

        private TagByQueryBitMapLookup GetTagBitMapIndexForQueryType(QueryType type)
        {
            switch (type)
            {
                case QueryType.LastActivityDate:
                    return tagsByLastActivityDateBitMapIndex;
                case QueryType.CreationDate:
                    return tagsByCreationDateBitMapIndex;
                case QueryType.Score:
                    return tagsByScoreBitMapIndex;
                case QueryType.ViewCount:
                    return tagsByViewCountBitMapIndex;
                case QueryType.AnswerCount:
                    return tagsByAnswerCountBitMapIndex;
                default:
                    throw new InvalidOperationException(string.Format("GetTagBitMapIndexForQueryType - Invalid query type {0}", (int)type));
            }
        }

        private Dictionary<string, TagWithPositions> CreateTagGroupings()
        {
            var tagGroupingTimer = Stopwatch.StartNew();
            var tempGroupedTags = new Dictionary<string, List<int>>();
            var position = 0;
            foreach (var qu in questions)
            {
                foreach (var tag in qu.Tags)
                {
                    if (tempGroupedTags.ContainsKey(tag) == false)
                    {
                        var tagWithPositions = new List<int>();
                        tagWithPositions.Add(position);
                        tempGroupedTags.Add(tag, tagWithPositions);
                    }
                    else
                    {
                        tempGroupedTags[tag].Add(position);
                    }
                }
                position++;
            }

            Dictionary<string, TagWithPositions> groupedTags = new Dictionary<string, TagWithPositions>();
            foreach (var item in tempGroupedTags.OrderByDescending(x => x.Value.Count))
            {
                var tagWithPositions = new TagWithPositions
                                            {
                                                Tag = item.Key,
                                                Positions = item.Value.ToArray(),
                                                Count = item.Value.Count
                                            };
                groupedTags.Add(item.Key, tagWithPositions);
            }

            // We end up with this Dictionary,
            // where the numbers are the array indexes of the questions in the rawQuestions array
            // {
            //   { "c#":   { "c#", 7193, int [7193] { 1, 4, 5, 6, 10, ..... } },
            //   { "java": { "java", 7100, int [7100] { 1, 2, 3, 7, 8, 9, ..... } },
            //   ....
            // }

            // Add in "_ALL_TAGS_" as a special case, so that we can walk through all tags (i.e. all qu's) in order)
            groupedTags.Add(ALL_TAGS_KEY, new TagWithPositions
            {
                Tag = ALL_TAGS_KEY,
                Count = questions.Count,
                Positions = Enumerable.Range(0, questions.Count).ToArray()
            });
            tagGroupingTimer.Stop();

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to group all the tags - Using {2:N2} MB ({3:N2} GB) of memory\n",
                                     tagGroupingTimer.Elapsed, tagGroupingTimer.ElapsedMilliseconds, mbUsed, mbUsed / 1024.0);
            return groupedTags;
        }

        private void CreateSortedLists(Dictionary<string, TagWithPositions> groupedTags, bool useAlternativeMethod = false)
        {
            //New faster sorting method (results from 2 different runs):
            //    Took 00:00:11.2802896 (11,280 ms) to sort the 191,030 arrays ALTERNATIVE method - Using 4,537.50 MB (4.43 GB) of memory
            //    Took 00:00:11.4762493 (11,476 ms) to sort the 191,030 arrays ALTERNATIVE method - Using 4,537.50 MB (4.43 GB) of memory
            //Old slower way of doing it (using a custom Comparer and indexing into the Questions array for each comparision, results from 2 different runs):
            //    Took 00:01:53.6553645 (113,655 ms) to sort the 191,030 arrays - Using 4,537.50 MB (4.43 GB) of memory
            //    Took 00:01:55.2932862 (115,293 ms) to sort the 191,030 arrays - Using 4,537.50 MB (4.43 GB) of memory
            var sortingTimer = Stopwatch.StartNew();
            foreach (var tag in groupedTags)
            {
                tagsByAnswerCount.Add(tag.Key, CreateSortedArrayForTag(tag.Value.Positions, QueryType.AnswerCount));
                tagsByCreationDate.Add(tag.Key, CreateSortedArrayForTag(tag.Value.Positions, QueryType.CreationDate));
                tagsByLastActivityDate.Add(tag.Key, CreateSortedArrayForTag(tag.Value.Positions, QueryType.LastActivityDate));
                tagsByScore.Add(tag.Key, CreateSortedArrayForTag(tag.Value.Positions, QueryType.Score));
                tagsByViewCount.Add(tag.Key, CreateSortedArrayForTag(tag.Value.Positions, QueryType.ViewCount));
            }
            sortingTimer.Stop();

            GC.Collect(2, GCCollectionMode.Forced);
            var memoryUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to sort the {2:N0} arrays {3}- Using {4:N2} MB ({5:N2} GB) of memory\n",
                                     sortingTimer.Elapsed, sortingTimer.ElapsedMilliseconds, groupedTags.Count * 5,
                                     useAlternativeMethod ? "ALTERNATIVE method " : "", memoryUsed, memoryUsed / 1024.0);
        }

        private int[] CreateSortedArrayForTag(int[] questionIds, QueryType queryType)
        {
            // Using alternative sorting method (that is almost x10 faster), inspired by Marc Gravell's SO answer, see
            // http://stackoverflow.com/questions/17399917/c-sharp-fastest-way-to-sort-array-of-primitives-and-track-their-indices/17399982#17399982
            var questionValues = new long[questionIds.Length];
            switch (queryType)
            {
                case QueryType.AnswerCount:
                    for (int i = 0; i < questionValues.Length; i++)
                        questionValues[i] = questions[questionIds[i]].AnswerCount ?? -1;
                    break;
                case QueryType.CreationDate:
                    for (int i = 0; i < questionValues.Length; i++)
                        questionValues[i] = questions[questionIds[i]].CreationDate.Ticks;
                    break;
                case QueryType.LastActivityDate:
                    for (int i = 0; i < questionValues.Length; i++)
                        questionValues[i] = questions[questionIds[i]].LastActivityDate.Ticks;
                    break;
                case QueryType.Score:
                    for (int i = 0; i < questionValues.Length; i++)
                        questionValues[i] = questions[questionIds[i]].Score ?? -1;
                    break;
                case QueryType.ViewCount:
                    for (int i = 0; i < questionValues.Length; i++)
                        questionValues[i] = questions[questionIds[i]].ViewCount ?? -1;
                    break;
            }

            // Create the indicies array (that is then sorted in the way as questionValues array)
            int[] indices = new int[questionValues.Length];
            for (int i = 0; i < indices.Length; i++)
                indices[i] = questionIds[i];

            // TODO it would be nicer if we could just sort in reverse order, but the overload doesn't seem to allow that!!
            //var reverseComparer = new Comparison<int>((i1, i2) => i1.CompareTo(i2));
            Array.Sort(questionValues, indices);

            // We want all the items to be sorted descending, i.e. highest/newest first
            Array.Reverse(indices);

            return indices;
        }

        private class TagWithPositions
        {
            public int Count { get; set; }
            public int[] Positions { get; set; }
            public string Tag { get; set; }
        }
    }
}
