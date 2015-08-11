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
using TagByQueryBitMapIndex = System.Collections.Generic.Dictionary<string, Ewah.EwahCompressedBitArray>;
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
        private readonly TagByQueryBitMapIndex tagsByAnswerCountBitMapIndex;
        private readonly TagByQueryBitMapIndex tagsByCreationDateBitMapIndex;
        private readonly TagByQueryBitMapIndex tagsByLastActivityDateBitMapIndex;
        private readonly TagByQueryBitMapIndex tagsByScoreBitMapIndex;
        private readonly TagByQueryBitMapIndex tagsByViewCountBitMapIndex;

        private readonly QueryProcessor queryProcessor;

        private readonly ComplexQueryProcessor complexQueryProcessor;

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
        /// <see cref="TagServer(List{Question}, TagLookup, Dictionary{QueryType, TagByQueryLookup}, Dictionary{QueryType, TagByQueryBitMapIndex})"/>
        /// </summary>
        public static TagServer CreateFromSerialisedData(List<Question> rawQuestions, string intermediateFilesFolder, bool deserialiseBitMapsIndexes = true)
        {
            var deserializeTimer = Stopwatch.StartNew();
            Logger.LogStartupMessage("Deserialisation folder: {0}", intermediateFilesFolder);
            var queryTypes = (QueryType[])Enum.GetValues(typeof(QueryType));
            var intermediateLookups = new Dictionary<QueryType, TagByQueryLookup>(queryTypes.Length);
            var intermediateBitMapIndexes = new Dictionary<QueryType, TagByQueryBitMapIndex>(queryTypes.Length);
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
                    var tempBitMapIndexes = new TagByQueryBitMapIndex();
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

        internal EwahCompressedBitArray CreateBitMapIndexForExcludedTags(CLR.HashSet<string> tagsToExclude, QueryType queryType)
        {
            var bitMapTimer = Stopwatch.StartNew();
            var tagLookupForQueryType = GetTagLookupForQueryType(queryType);
            var bitMap = new EwahCompressedBitArray();

            var collectIdsTimer = Stopwatch.StartNew();
            var excludedQuestionIds = new HashSet<int>();
            foreach (var tag in tagsToExclude)
            {
                foreach (var id in tagLookupForQueryType[tag])
                {
                    excludedQuestionIds.Add(id);
                }
            }
            collectIdsTimer.Stop();

            // At the end we need to have the BitMap Set (i.e. 1) in places where you CAN use the question, i.e. it's NOT excluded
            // That way we can efficiently apply the exclusions by ANDing this BitMap to the previous results

            var setBitsTimer = Stopwatch.StartNew();
            var reverseMode = excludedQuestionIds.Count < (questions.Count / 2);

            var allQuestions = tagLookupForQueryType[ALL_TAGS_KEY];
            for (int index = 0; index < allQuestions.Length; index++)
            {
                var questionId = allQuestions[index];
                bool wasSet = true;
                if (reverseMode && excludedQuestionIds.Contains(questionId))
                    wasSet = bitMap.Set(index); // Set where you CAN'T use a question, but at the end NOT the whole BitMap (see below)
                else if (reverseMode == false && excludedQuestionIds.Contains(questionId) == false)
                    wasSet = bitMap.Set(index); // Directly set where you CAN use the question

                if (wasSet == false)
                    Logger.LogStartupMessage("Error, unable to set bit {0:N0} (SizeInBits = {1:N0})", index, bitMap.SizeInBits);
            }
            setBitsTimer.Stop();

            var alternativeBitSetTimer = Stopwatch.StartNew();
            var bitArrayLength = CLR.BitHelper.ToIntArrayLength(questions.Count);
            var bitHelperArray = new int[bitArrayLength];
            var regularBitSet = new CLR.BitHelper(bitHelperArray, bitArrayLength);
            for (int index = 0; index < allQuestions.Length; index++)
            {
                var questionId = allQuestions[index];
                if (reverseMode && excludedQuestionIds.Contains(questionId))
                    regularBitSet.MarkBit(index); // Set where you CAN'T use a question, but at the end NOT the whole BitMap (see below)
                else if (reverseMode == false && excludedQuestionIds.Contains(questionId) == false)
                    regularBitSet.MarkBit(index); // Directly set where you CAN use the question
            }
            alternativeBitSetTimer.Stop();

            var tidyUpTimer = Stopwatch.StartNew();
            bitMap.SetSizeInBits(questions.Count, defaultvalue: false);
            bitMap.Shrink();
            tidyUpTimer.Stop();

            var notTimer = Stopwatch.StartNew();
            if (reverseMode)
            {
                bitMap.Not(); // in-place
                regularBitSet.Not(); // in-place
            }
            notTimer.Stop();

            bitMapTimer.Stop();

            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to collect {2:N0} Question Ids from {3:N0} Tags",
                                     collectIdsTimer.Elapsed, collectIdsTimer.ElapsedMilliseconds, excludedQuestionIds.Count, tagsToExclude.Count);
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to set {2:N0} bits {3}",
                                     setBitsTimer.Elapsed, setBitsTimer.ElapsedMilliseconds,
                                     reverseMode ? ((ulong)questions.Count - bitMap.GetCardinality()) : bitMap.GetCardinality(),
                                     reverseMode ? "(in REVERSE mode)" : "");
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to set {2:N0} ({3:N0}) bits using ALTERNATIVE mode {4}",
                                     alternativeBitSetTimer.Elapsed, alternativeBitSetTimer.ElapsedMilliseconds,
                                     (reverseMode ? (questions.Count - regularBitSet.GetCardinality()) : regularBitSet.GetCardinality()),
                                     regularBitSet.GetCardinality(),
                                     reverseMode ? "(in REVERSE mode)" : "");
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to tidy-up the Bit Map (SetSizeInBits(..) and Shrink()), Size={2:N0} bytes ({3:N2} MB)",
                                     tidyUpTimer.Elapsed, tidyUpTimer.ElapsedMilliseconds, bitMap.SizeInBytes, bitMap.SizeInBytes / 1024.0 / 1024.0);
            if (reverseMode)
                Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to do a NOT on the BitMap", notTimer.Elapsed, notTimer.ElapsedMilliseconds);

            using (Utils.SetConsoleColour(ConsoleColor.DarkYellow))
            {
                Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to create BitMap from {2:N0} Tags ({3:N0} Qu Ids), Cardinality={4:N0} ({5:N0}) {6}\n",
                                         bitMapTimer.Elapsed, bitMapTimer.ElapsedMilliseconds,
                                         tagsToExclude.Count,
                                         excludedQuestionIds.Count,
                                         bitMap.GetCardinality(),
                                         (ulong)questions.Count - bitMap.GetCardinality(),
                                         reverseMode ? "REVERSE mode" : "");
            }

            return bitMap;
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

        internal void TestBitMapIndexes(string tag1, string tag2, QueryType queryType, string @operator)
        {
            var fieldFetcher = queryTypeLookup[queryType];
            var bitMap = GetTagBitMapIndexForQueryType(queryType);
            var questionLookup = GetTagLookupForQueryType(queryType)[ALL_TAGS_KEY];

            Logger.Log("Tag \"{0}\" is in {1:N0} Questions, Tag \"{2}\" is in {3:N0} Questions", tag1, allTags[tag1], tag2, allTags[tag2]);

            //PrintResults(Enumerable.Range(0, questionLookup.Length), questionLookup, ALL_TAGS_KEY, queryType, fieldFetcher);
            //PrintResults(bitMap[tag1], questionLookup, tag1, queryType, fieldFetcher);
            //PrintResults(bitMap[tag2], questionLookup, tag2, queryType, fieldFetcher);

            var timer = Stopwatch.StartNew();
            var tag1BitMap = bitMap[tag1];
            var tag2BitMap = bitMap[tag2];
            EwahCompressedBitArray bitMapResult = new EwahCompressedBitArray();

            if (@operator == "OR")
            {
                bitMapResult = tag1BitMap.Or(tag2BitMap);
            }
            else if (@operator == "OR NOT")
            {
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
            }
            else if (@operator == "AND")
            {
                bitMapResult = tag1BitMap.And(tag2BitMap);
            }
            else if (@operator == "AND NOT")
            {
                var cloneTimer = Stopwatch.StartNew();
                var notTag2BitMap = (EwahCompressedBitArray)tag2BitMap.Clone();
                cloneTimer.Stop();

                var notTimer = Stopwatch.StartNew();
                notTag2BitMap.Not();
                notTimer.Stop();

                var andTimer = Stopwatch.StartNew();
                bitMapResult = tag1BitMap.And(notTag2BitMap);
                andTimer.Stop();

                Logger.Log("CLONING took {0:N2} ms, NOT took {1:N2} ms, AND took {2:N2} ms",
                           cloneTimer.Elapsed.TotalMilliseconds, notTimer.Elapsed.TotalMilliseconds, andTimer.Elapsed.TotalMilliseconds);
            }

            timer.Stop();

            using (Utils.SetConsoleColour(ConsoleColor.DarkYellow))
            {
                Logger.Log("Took {0,5:N2} ms to calculate \"{1} {2} {3}\"", timer.Elapsed.TotalMilliseconds, tag1, @operator, tag2);
            }
            //PrintResults(bitMapResult, questionLookup, string.Format("{0} {1} {2)", tag1, @operator, tag2), queryType, fieldFetcher);
            Logger.Log();
        }

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

        /// <summary>
        /// Private constructor that is used when creating the Tag Server from SCRATCH (<see cref="CreateFromScratchAndSaveToDisk"/>)
        /// </summary>
        /// <param name="questionsList"></param>
        private TagServer(List<Question> questionsList)
        {
            questions = questionsList;
            queryProcessor = new QueryProcessor(questions, type => GetTagLookupForQueryType(type));
            complexQueryProcessor = new ComplexQueryProcessor(questions, type => GetTagLookupForQueryType(type));

            var groupedTags = CreateTagGroupings();
            allTags = groupedTags.ToDictionary(t => t.Key, t => t.Value.Count);

            // These have to be initialised in the ctor, so they can remain readonly
            tagsByAnswerCount = new TagByQueryLookup(groupedTags.Count);
            tagsByCreationDate = new TagByQueryLookup(groupedTags.Count);
            tagsByLastActivityDate = new TagByQueryLookup(groupedTags.Count);
            tagsByScore = new TagByQueryLookup(groupedTags.Count);
            tagsByViewCount = new TagByQueryLookup(groupedTags.Count);

            // These have to be initialised in the ctor, so they can remain readonly
            tagsByAnswerCountBitMapIndex = new TagByQueryBitMapIndex(groupedTags.Count);
            tagsByCreationDateBitMapIndex = new TagByQueryBitMapIndex(groupedTags.Count);
            tagsByLastActivityDateBitMapIndex = new TagByQueryBitMapIndex(groupedTags.Count);
            tagsByScoreBitMapIndex = new TagByQueryBitMapIndex(groupedTags.Count);
            tagsByViewCountBitMapIndex = new TagByQueryBitMapIndex(groupedTags.Count);

            CreateSortedLists(groupedTags, useAlternativeMethod: true);

            Logger.LogStartupMessage(new string('#', Console.WindowWidth));
            CreateBitMapIndexes(groupedTags);
            Logger.LogStartupMessage(new string('#', Console.WindowWidth));

            ValidateTagOrdering();
            ValidateBitMapIndexOrdering();

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("After TagServer created - Using {0:N2} MB ({1:N2} GB) of memory in total\n", mbUsed, mbUsed / 1024.0);
        }

        /// <summary>
        /// Private constructor that is used when creating the Tag Server from previously serialised data (<see cref="CreateFromSerialisedData"/>)
        /// </summary>
        private TagServer(List<Question> questionsList, TagLookup allTags,
                          Dictionary<QueryType, TagByQueryLookup> intermediateLookups,
                          Dictionary<QueryType, TagByQueryBitMapIndex> intermediateBitMapIndexes)
        {
            questions = questionsList;
            queryProcessor = new QueryProcessor(questions, type => GetTagLookupForQueryType(type));
            complexQueryProcessor = new ComplexQueryProcessor(questions, type => GetTagLookupForQueryType(type));
            this.allTags = allTags;

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

        #region QueryApiPassedThruToQueryProcessor

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

        public List<Question> BooleanQueryWithExclusionsLINQVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.BooleanQueryWithExclusionsLINQVersion(type, tag, excludedTags, pageSize, skip);
        }

        public List<Question> BooleanQueryWithExclusionsFastVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.BooleanQueryWithExclusionsFastVersion(type, tag, excludedTags, pageSize, skip);
        }

        public List<Question> BooleanQueryWithExclusionsFastAlternativeVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.BooleanQueryWithExclusionsFastAlternativeVersion(type, tag, excludedTags, pageSize, skip);
        }

        public List<Question> BooleanQueryWithExclusionsBloomFilterVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.BooleanQueryWithExclusionsBloomFilterVersion(type, tag, excludedTags, pageSize, skip);
        }

#endregion QueryApiPassedThruToQueryProcessor

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

        private TagByQueryBitMapIndex GetTagBitMapIndexForQueryType(QueryType type)
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

            // Add in "_ALL_TAGS_" as a special case, so that we can walk through all tags (all qu's) in order)
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
            // Using alternative sorting method, inspired by Marc Gravell's SO answer, see
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

            // We want all the items to be sorted descending, i.e. highest first
            Array.Reverse(indices);

            return indices;
        }

        private void CreateBitMapIndexes(Dictionary<string, TagWithPositions> groupedTags)
        {
            // First create all the BitMap Indexes we'll need, one per/Tag, per/QueryType
            var bitSetsTimer = Stopwatch.StartNew();
            var tagsToUse = GetTagsToUseForBitMapIndexes(minQuestionsPerTag: 0);
            //var tagsToUse = GetTagsToUseForBitMapIndexes(minQuestionsPerTag: 500); // 3,975 Tags with MORE than 500 questions
            //var tagsToUse = GetTagsToUseForBitMapIndexes(minQuestionsPerTag: 1000); // 2,397 Tags with MORE than 1,000 questions
            //var tagsToUse = GetTagsToUseForBitMapIndexes(minQuestionsPerTag: 50000); // 48 Tags with MORE than 50,000 questions
            foreach (var tagToUse in tagsToUse)
            {
                tagsByAnswerCountBitMapIndex.Add(tagToUse, new EwahCompressedBitArray());
                tagsByCreationDateBitMapIndex.Add(tagToUse, new EwahCompressedBitArray());
                tagsByLastActivityDateBitMapIndex.Add(tagToUse, new EwahCompressedBitArray());
                tagsByScoreBitMapIndex.Add(tagToUse, new EwahCompressedBitArray());
                tagsByViewCountBitMapIndex.Add(tagToUse, new EwahCompressedBitArray());
            }

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("Created {0:N0} BitMap Indexes in total (one per/Tag, per/QueryType, for {1:N0} Tags) - Using {2:N2} MB ({3:N2} GB) of memory\n",
                                     tagsToUse.Length * 5, tagsToUse.Length, mbUsed, mbUsed / 1024.0);

            PopulateBitMapIndexes();
            bitSetsTimer.Stop();

            GC.Collect(2, GCCollectionMode.Forced);
            var memoryUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("\nTook {0} ({1,6:N0} ms) in Total to create {2:N0} BitMap Indexes - Using {3:N2} MB ({4:N2} GB) of memory in total\n",
                                     bitSetsTimer.Elapsed, bitSetsTimer.ElapsedMilliseconds, tagsToUse.Length * 5, memoryUsed, memoryUsed / 1024.0);

            var shrinkTimer = Stopwatch.StartNew();
            PostProcessBitMapIndexes(tagsToUse);
            shrinkTimer.Stop();

            GC.Collect(2, GCCollectionMode.Forced);
            memoryUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to shrink {2:N0} BitMap Indexes - Now using {3:N2} MB ({4:N2} GB) of memory in total\n",
                                     shrinkTimer.Elapsed, shrinkTimer.ElapsedMilliseconds, tagsToUse.Length * 5, memoryUsed, memoryUsed / 1024.0);
        }

        private void PopulateBitMapIndexes()
        {
            // TODO see if we can use "Add(long newdata)" or "AddStreamOfEmptyWords(bool v, long number)" to make this faster?!
            foreach (QueryType queryType in Enum.GetValues(typeof(QueryType)))
            {
                var questionsForQuery = GetTagLookupForQueryType(queryType)[ALL_TAGS_KEY];
                var sanityCheck = new Dictionary<string, int>();
                var bitSetsForQuery = GetTagBitMapIndexForQueryType(queryType);
                if (bitSetsForQuery.Count == 0)
                    continue;

                var populationTimer = Stopwatch.StartNew();
                foreach (var item in questionsForQuery.Select((QuestionId, Index) => new { QuestionId, Index }))
                {
                    var question = questions[item.QuestionId];
                    foreach (var tag in question.Tags)
                    {
                        if (bitSetsForQuery.ContainsKey(tag) == false)
                            continue;

                        bitSetsForQuery[tag].Set(item.Index);

                        if (sanityCheck.ContainsKey(tag))
                            sanityCheck[tag]++;
                        else
                            sanityCheck.Add(tag, 1);
                    }
                }
                populationTimer.Stop();
                Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to populate BitMap Index for {2}",
                                         populationTimer.Elapsed, populationTimer.ElapsedMilliseconds, queryType);

                foreach (var item in sanityCheck.OrderByDescending(t => t.Value))
                {
                    var firstError = true;
                    if (allTags[item.Key] != item.Value)
                    {
                        if (firstError)
                        {
                            Logger.LogStartupMessage("Errors in BitMap Index for {0}:", queryType);
                            firstError = false;
                        }

                        var errorText =
                            allTags[item.Key] != item.Value ?
                                string.Format(" *** Error expected {0}, but got {1} ***", allTags[item.Key], item.Value) : "";
                        Logger.LogStartupMessage("\t[{0}, {1:N0}]{2}", item.Key, item.Value, errorText);
                    }
                }
            }
        }

        private void PostProcessBitMapIndexes(string[] tagsToUse)
        {
            // Ensure that the BitMap Indexes represent the entire count of questions and then shrink them to their smallest possible size
            foreach (var tagToUse in tagsToUse)
            {
                tagsByAnswerCountBitMapIndex[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByAnswerCountBitMapIndex[tagToUse].Shrink();

                tagsByCreationDateBitMapIndex[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByCreationDateBitMapIndex[tagToUse].Shrink();

                tagsByLastActivityDateBitMapIndex[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByLastActivityDateBitMapIndex[tagToUse].Shrink();

                tagsByScoreBitMapIndex[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByScoreBitMapIndex[tagToUse].Shrink();

                tagsByViewCountBitMapIndex[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByViewCountBitMapIndex[tagToUse].Shrink();
            }
        }

        private string[] GetTagsToUseForBitMapIndexes(int minQuestionsPerTag)
        {
            // There are     48 Tags with MORE than 50,000 questions
            // There are    113 Tags with MORE than 25,000 questions
            // There are    306 Tags with MORE than 10,000 questions
            // There are    607 Tags with MORE than  5,000 questions
            // There are  1,155 Tags with MORE than  2,500 questions
            // There are  2,397 Tags with MORE than  1,000 questions
            // There are  3,975 Tags with MORE than    500 questions
            // There are  7,230 Tags with MORE than    200 questions
            // There are 10,814 Tags with MORE than    100 questions
            // There are 15,691 Tags with MORE than     50 questions
            // There are 27,658 Tags with MORE than     10 questions
            return allTags.OrderByDescending(t => t.Value)
                          .Where(t => t.Value > minQuestionsPerTag)
                          .Select(t => t.Key)
                          .ToArray();
        }

        private void ValidateTagOrdering()
        {
            var validator = new Validator(questions);
            var validationTimer = Stopwatch.StartNew();

            validator.ValidateItems(GetTagLookupForQueryType(QueryType.LastActivityDate).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => qu.LastActivityDate <= prev.LastActivityDate,
                                    "Tags-" + QueryType.LastActivityDate);

            validator.ValidateItems(GetTagLookupForQueryType(QueryType.CreationDate).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => qu.CreationDate <= prev.CreationDate,
                                    "Tags-" + QueryType.CreationDate);

            validator.ValidateItems(GetTagLookupForQueryType(QueryType.Score).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => Nullable.Compare(qu.Score, prev.Score) <= 0,
                                    "Tags-" + QueryType.Score);

            validator.ValidateItems(GetTagLookupForQueryType(QueryType.ViewCount).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => Nullable.Compare(qu.ViewCount, prev.ViewCount) <= 0,
                                    "Tags-" + QueryType.ViewCount);

            validator.ValidateItems(GetTagLookupForQueryType(QueryType.AnswerCount).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => Nullable.Compare(qu.AnswerCount, prev.AnswerCount) <= 0,
                                    "Tags-" + QueryType.AnswerCount);

            validationTimer.Stop();
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to VALIDATE the {2:N0} arrays\n",
                                     validationTimer.Elapsed, validationTimer.ElapsedMilliseconds, allTags.Count * 5);
        }

        private void ValidateBitMapIndexOrdering()
        {
            var validator = new Validator(questions);
            var validationTimer = Stopwatch.StartNew();

            validator.ValidateItems(GetTagBitMapIndexForQueryType(QueryType.LastActivityDate).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => qu.LastActivityDate <= prev.LastActivityDate,
                                    "BitMaps-" + QueryType.LastActivityDate,
                                    questionLookup: GetTagLookupForQueryType(QueryType.LastActivityDate)[ALL_TAGS_KEY]);

            validator.ValidateItems(GetTagBitMapIndexForQueryType(QueryType.CreationDate).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => qu.CreationDate <= prev.CreationDate,
                                    "BitMaps-" + QueryType.CreationDate,
                                    questionLookup: GetTagLookupForQueryType(QueryType.CreationDate)[ALL_TAGS_KEY]);

            validator.ValidateItems(GetTagBitMapIndexForQueryType(QueryType.Score).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => Nullable.Compare(qu.Score, prev.Score) <= 0,
                                    "BitMaps-" + QueryType.Score,
                                    questionLookup: GetTagLookupForQueryType(QueryType.Score)[ALL_TAGS_KEY]);

            validator.ValidateItems(GetTagBitMapIndexForQueryType(QueryType.ViewCount).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => Nullable.Compare(qu.ViewCount, prev.ViewCount) <= 0,
                                    "BitMaps-" + QueryType.ViewCount,
                                    questionLookup: GetTagLookupForQueryType(QueryType.ViewCount)[ALL_TAGS_KEY]);

            validator.ValidateItems(GetTagBitMapIndexForQueryType(QueryType.AnswerCount).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => Nullable.Compare(qu.AnswerCount, prev.AnswerCount) <= 0,
                                    "BitMaps-" + QueryType.AnswerCount,
                                    questionLookup: GetTagLookupForQueryType(QueryType.AnswerCount)[ALL_TAGS_KEY]);

            validationTimer.Stop();
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to VALIDATE all the {2:N0} Bit Map Indexes\n",
                                     validationTimer.Elapsed, validationTimer.ElapsedMilliseconds, allTags.Count * 5);
        }

        private class TagWithPositions
        {
            public int Count { get; set; }
            public int[] Positions { get; set; }
            public string Tag { get; set; }
        }
    }
}
