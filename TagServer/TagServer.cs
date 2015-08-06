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
using TagByQueryLookupBitSet = System.Collections.Generic.Dictionary<string, Ewah.EwahCompressedBitArray>;
using TagLookup = System.Collections.Generic.Dictionary<string, int>;

namespace StackOverflowTagServer
{
    public class TagServer
    {
        public delegate void LogAction(string format, params object[] args);

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

        // GetTagBitSetForQueryType(QueryType type) maps these Dictionaries to a QueryType (enum)
        private readonly TagByQueryLookupBitSet tagsByAnswerCountBitSet;
        private readonly TagByQueryLookupBitSet tagsByCreationDateBitSet;
        private readonly TagByQueryLookupBitSet tagsByLastActivityDateBitSet;
        private readonly TagByQueryLookupBitSet tagsByScoreBitSet;
        private readonly TagByQueryLookupBitSet tagsByViewCountBitSet;

        private readonly QueryProcessor queryProcessor;

        private readonly ComplexQueryProcessor complexQueryProcessor;

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

                var bitMapIndex = tagServer.GetTagBitSetForQueryType(type);
                if (bitMapIndex.Count == 0)
                    continue;

                var bitMapIndexFileName = String.Format("intermediate-Compressed-BitMap-{0}.bin", type);
                Serialisation.SerialiseBitMapIndexToDisk(bitMapIndexFileName, intermediateFilesFolder, bitMapIndex);

                // Sanity-check, de-serialise the data we've just written to disk
                Serialisation.SerialiseBitMapIndexFromDisk(bitMapIndexFileName, intermediateFilesFolder);

                Logger.LogStartupMessage();
            }

            // Now write out the AllTags Lookup, Tag -> Count (i.e. "C#" -> 579,321, "Java" -> 560,432)
            Serialisation.SerialiseToDisk(AllTagsFileName, intermediateFilesFolder, tagServer.AllTags);
            serializeTimer.Stop();
            Logger.LogStartupMessage("\nTook {0} (in TOTAL) to serialise the intermediate data TO disk\n", serializeTimer.Elapsed);

            return tagServer;
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
            tagsByAnswerCountBitSet = new TagByQueryLookupBitSet(groupedTags.Count);
            tagsByCreationDateBitSet = new TagByQueryLookupBitSet(groupedTags.Count);
            tagsByLastActivityDateBitSet = new TagByQueryLookupBitSet(groupedTags.Count);
            tagsByScoreBitSet = new TagByQueryLookupBitSet(groupedTags.Count);
            tagsByViewCountBitSet = new TagByQueryLookupBitSet(groupedTags.Count);

            CreateSortedLists(groupedTags, useAlternativeMethod: true);

            Logger.LogStartupMessage(new string('#', Console.WindowWidth));
            CreateBitMapIndexes(groupedTags);
            Logger.LogStartupMessage(new string('#', Console.WindowWidth));

            //ValidateTagOrdering();
            //ValidateBitSetOrdering();

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("After TagServer created - Using {0:N2} MB ({1:N2} GB) of memory in total\n", mbUsed, mbUsed / 1024.0);
        }

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
        /// Factory method to create a <see cref="TagServer"/>, uses the private Constructor
        /// <see cref="TagServer(List{Question}, TagLookup, Dictionary{QueryType, TagByQueryLookup}, Dictionary{QueryType, TagByQueryLookupBitSet})"/>
        /// </summary>
        public static TagServer CreateFromSerialisedData(List<Question> rawQuestions, string intermediateFilesFolder)
        {
            var deserializeTimer = Stopwatch.StartNew();
            Logger.LogStartupMessage("Deserialisation folder: {0}", intermediateFilesFolder);
            var queryTypes = (QueryType[])Enum.GetValues(typeof(QueryType));
            var intermediateLookups = new Dictionary<QueryType, TagByQueryLookup>(queryTypes.Length);
            var intermediateBitSets = new Dictionary<QueryType, TagByQueryLookupBitSet>(queryTypes.Length);
            foreach (QueryType type in queryTypes)
            {
                var tagLookupFileName = "intermediate-Lookup-" + type + ".bin";
                var tempLookup = Serialisation.DeserialiseFromDisk<TagByQueryLookup>(tagLookupFileName, intermediateFilesFolder);
                intermediateLookups.Add(type, tempLookup);
                Logger.LogStartupMessage("{0,20} contains {1:N0} Tag Lookups", type, tempLookup.Count);

                var bitSetFileName = String.Format("intermediate-CompressedBitSet-{0}.bin", type);
                var tempBitSet = Serialisation.DeserialiseFromDisk<TagByQueryLookupBitSet>(bitSetFileName, intermediateFilesFolder);
                intermediateBitSets.Add(type, tempBitSet);
                Logger.LogStartupMessage("{0,20} contains {1:N0} Tag BitSets", type, tempBitSet.Count);
            }
            // Now fetch from disk the AllTags Lookup, Tag -> Count (i.e. "C#" -> 579,321, "Java" -> 560,432)
            var allTags = Serialisation.DeserialiseFromDisk<TagLookup>(AllTagsFileName, intermediateFilesFolder);
            deserializeTimer.Stop();
            Logger.LogStartupMessage("\nTook {0} (in TOTAL) to DE-serialise the intermediate data FROM disk\n", deserializeTimer.Elapsed);

            return new TagServer(rawQuestions, allTags, intermediateLookups, intermediateBitSets);
        }

        /// <summary>
        /// Private constructor that is used when creating the Tag Server from previously serialised data (<see cref="CreateFromSerialisedData"/>)
        /// </summary>
        private TagServer(List<Question> questionsList, TagLookup allTags,
                          Dictionary<QueryType, TagByQueryLookup> intermediateLookups,
                          Dictionary<QueryType, TagByQueryLookupBitSet> intermediateBitSets)
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
            tagsByAnswerCountBitSet = intermediateBitSets[QueryType.AnswerCount];
            tagsByCreationDateBitSet = intermediateBitSets[QueryType.CreationDate];
            tagsByLastActivityDateBitSet = intermediateBitSets[QueryType.LastActivityDate];
            tagsByScoreBitSet = intermediateBitSets[QueryType.Score];
            tagsByViewCountBitSet = intermediateBitSets[QueryType.ViewCount];

            // This takes a while, maybe don't do it when using Intermediate results (that have already has this check?)
            //ValidateTagOrdering();
            //ValidateBitSetOrdering();

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("After TagServer created - Using {0:N2} MB ({1:N2} GB) of memory in total\n", mbUsed, mbUsed / 1024.0);
        }

        public int TotalCount(QueryType type, string tag)
        {
            TagByQueryLookup queryInfo = GetTagLookupForQueryType(type);
            return queryInfo[tag].Length;
        }

        private static TagByQueryLookupBitSet CreateBitSets(int numBitSetsToCreate, int numItems, int jumpsPerLoop)
        {
            var memoryUsageBefore = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            var timer = Stopwatch.StartNew();
            var dictionaryBitSet = new TagByQueryLookupBitSet();
            var numBitsSet = 0;
            for (int i = 0; i < numBitSetsToCreate; i++)
            {
                var bitMapIndex = new Ewah.EwahCompressedBitArray();
                numBitsSet = 0;
                for (int j = i; j < numItems; j += jumpsPerLoop)
                {
                    bitMapIndex.Set(j);
                    numBitsSet++;
                    // TODO work out a better way of doing this sanity-check
                    //if (bitSet[j] == false)
                    //    Logger.LogStartupMessage("ERROR as posn[{0}] in BitMap Index[{1}]", j, i);
                }
                dictionaryBitSet.Add(i.ToString(), bitMapIndex);
            }
            timer.Stop();
            Logger.LogStartupMessage("\nTook {0:N2} msecs to create {1} BitSets, each with {2:N0} individual Bits that have been set",
                                     timer.Elapsed.TotalMilliseconds, numBitSetsToCreate, numBitsSet);

            var memoryUsageAfter = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("Using {0:N2} MB", memoryUsageAfter - memoryUsageBefore);

            return dictionaryBitSet;
        }

        internal void TestBitSetsOnDeserialisedQuestionData()
        {
            //var bitSetTimer = Stopwatch.StartNew();
            //var byAnswerCount = GetTagBitSetForQueryType(QueryType.AnswerCount);
            //var cSharp = byAnswerCount["c#"];
            //var jQuery = byAnswerCount["jquery"];
            //var takeValue = 3; // 10; // for hex
            //var skipValue = 5;
            //string formatString = "X8", spacer = " ";

            ////Logger.Log("cSharp:        {0}",
            ////           //String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => i.ToString(formatString))),
            ////           String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => Convert.ToString(i, 2).PadLeft(32, '0'))));
            ////Logger.Log("jQuery:        {0}",
            ////           //String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => i.ToString(formatString))),
            ////            String.Join(spacer, jQuery.InternalArray.Skip(skipValue).Take(takeValue).Select(i => Convert.ToString(i, 2).PadLeft(32, '0'))));

            //jQuery.Not(); // Edits in-place, for real queries we NEED to make a copy
            //              //Logger.Log("NOT jQuery:    {0}",
            //              //           //String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => i.ToString(formatString))),
            //              //           String.Join(spacer, jQuery.InternalArray.Skip(skipValue).Take(takeValue).Select(i => Convert.ToString(i, 2).PadLeft(32, '0'))));

            //cSharp.Or(jQuery); // Edits in-place, for real queries we NEED to make a copy
            //                   //Logger.Log("cSharp OR (NOT jQuery)");

            ////cSharp.And(jQuery); // Edits in-place, for real queries we NEED to make a copy
            ////Logger.Log("cSharp AND (NOT jQuery)");

            ////Logger.Log("cSharp Result: {0}",
            ////           //String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => i.ToString(formatString))),
            ////           String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => Convert.ToString(i, 2).PadLeft(32, '0'))));

            //bitSetTimer.Stop();

            //Logger.Log("Took {0} ({1:N0} ms) to do C# Or (Not jQuery)\n", bitSetTimer.Elapsed, bitSetTimer.ElapsedMilliseconds);
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

        private TagByQueryLookupBitSet GetTagBitSetForQueryType(QueryType type)
        {
            switch (type)
            {
                case QueryType.LastActivityDate:
                    return tagsByLastActivityDateBitSet;
                case QueryType.CreationDate:
                    return tagsByCreationDateBitSet;
                case QueryType.Score:
                    return tagsByScoreBitSet;
                case QueryType.ViewCount:
                    return tagsByViewCountBitSet;
                case QueryType.AnswerCount:
                    return tagsByAnswerCountBitSet;
                default:
                    throw new InvalidOperationException(string.Format("GetTagBitSetForQueryType - Invalid query type {0}", (int)type));
            }
        }

        private Dictionary<string, TagWithPositions> CreateTagGroupings()
        {
            var tagGroupingTimer = Stopwatch.StartNew();
            // TODO Could **possibly** optimise this by doing it without LINQ, or maybe
            // just use LINQ Optimiser to do it for us (it currently takes 30 seconds)
            var groupedTags = questions.SelectMany((qu, n) => qu.Tags.Select(t => new
                                                                {
                                                                    Tag = t,
                                                                    Position = n
                                                                }),
                                                   (qu, tag) => tag)
                                       .ToLookup(x => x.Tag)
                                       .Select(x => new TagWithPositions()
                                                    {
                                                        Tag = x.Key,
                                                        Count = x.Count(),
                                                        Positions = x.Select(y => y.Position).ToArray()
                                                    })
                                       .OrderByDescending(x => x.Count)
                                       .ToDictionary(x => x.Tag);

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
            //New faster sorting method:
            //    Took 00:00:11.2802896 (11,280 ms) to sort the 191,030 arrays ALTERNATIVE method - Using 4,537.50 MB (4.43 GB) of memory
            //    Took 00:00:11.4762493 (11,476 ms) to sort the 191,030 arrays ALTERNATIVE method - Using 4,537.50 MB (4.43 GB) of memory
            //Old slower way of doing it (using a custom Comparer and indexing into the Questions array for each comparision):
            //    Took 00:01:53.6553645 (113,655 ms) to sort the 191,030 arrays - Using 4,537.50 MB (4.43 GB) of memory
            //    Took 00:01:55.2932862 (115,293 ms) to sort the 191,030 arrays - Using 4,537.50 MB (4.43 GB) of memory
            var sortingTimer = Stopwatch.StartNew();
            foreach (var tag in groupedTags)
            {
                tagsByAnswerCount.Add(tag.Key, CreateSortedArrayForTagAlternativeMethod(tag.Value.Positions, QueryType.AnswerCount));
                tagsByCreationDate.Add(tag.Key, CreateSortedArrayForTagAlternativeMethod(tag.Value.Positions, QueryType.CreationDate));
                tagsByLastActivityDate.Add(tag.Key, CreateSortedArrayForTagAlternativeMethod(tag.Value.Positions, QueryType.LastActivityDate));
                tagsByScore.Add(tag.Key, CreateSortedArrayForTagAlternativeMethod(tag.Value.Positions, QueryType.Score));
                tagsByViewCount.Add(tag.Key, CreateSortedArrayForTagAlternativeMethod(tag.Value.Positions, QueryType.ViewCount));
            }
            sortingTimer.Stop();

            GC.Collect(2, GCCollectionMode.Forced);
            var memoryUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to sort the {2:N0} arrays {3}- Using {4:N2} MB ({5:N2} GB) of memory\n",
                sortingTimer.Elapsed, sortingTimer.ElapsedMilliseconds, groupedTags.Count * 5,
                useAlternativeMethod ? "ALTERNATIVE method " : "", memoryUsed, memoryUsed / 1024.0);
        }

        private int[] CreateSortedArrayForTagAlternativeMethod(int[] positions, QueryType queryType)
        {
            // Using alternative sorting method, inspired by Marc Gravell's SO answer, see
            // http://stackoverflow.com/questions/17399917/c-sharp-fastest-way-to-sort-array-of-primitives-and-track-their-indices/17399982#17399982
            var unsortedArray = new long[positions.Length];
            switch (queryType)
            {
                case QueryType.AnswerCount:
                    for (int i = 0; i < unsortedArray.Length; i++)
                        unsortedArray[i] = questions[positions[i]].AnswerCount ?? -1;
                    break;
                case QueryType.CreationDate:
                    for (int i = 0; i < unsortedArray.Length; i++)
                        unsortedArray[i] = questions[positions[i]].CreationDate.Ticks;
                    break;
                case QueryType.LastActivityDate:
                    for (int i = 0; i < unsortedArray.Length; i++)
                        unsortedArray[i] = questions[positions[i]].LastActivityDate.Ticks;
                    break;
                case QueryType.Score:
                    for (int i = 0; i < unsortedArray.Length; i++)
                        unsortedArray[i] = questions[positions[i]].Score ?? -1;
                    break;
                case QueryType.ViewCount:
                    for (int i = 0; i < unsortedArray.Length; i++)
                        unsortedArray[i] = questions[positions[i]].ViewCount ?? -1;
                    break;
            }

            int[] indices = new int[unsortedArray.Length];
            for (int i = 0; i < indices.Length; i++)
                indices[i] = positions[i];
            // TODO it would be nicer if we could just sort in reverse order, but the overload doesn't seem to allow that!!
            //var reverserComparer = new Comparison<int>((i1, i2) => i1.CompareTo(i2));
            Array.Sort(unsortedArray, indices);
            // We want all the items to be sorted descending, i.e. highest first
            Array.Reverse(indices);
            return indices; // this is now sorted!!
        }

        private void CreateBitMapIndexes(Dictionary<string, TagWithPositions> groupedTags)
        {
            // First create all the BitSets we'll need, one per/Tag, per/QueryType
            var bitSetsTimer = Stopwatch.StartNew();
            var tagsToUse = GetTagsToUseForBitSets(minQuestionsPerTag: 0);
            //var tagsToUse = GetTagsToUseForBitSets(minQuestionsPerTag: 500); // 3,975 Tags with MORE than 500 questions
            //var tagsToUse = GetTagsToUseForBitSets(minQuestionsPerTag: 1000); // 2,397 Tags with MORE than 1,000 questions
            //var tagsToUse = GetTagsToUseForBitSets(minQuestionsPerTag: 50000); // 48 Tags with MORE than 50,000 questions
            foreach (var tagToUse in tagsToUse)
            {
                tagsByAnswerCountBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
                tagsByCreationDateBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
                tagsByLastActivityDateBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
                tagsByScoreBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
                tagsByViewCountBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
            }

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Logger.LogStartupMessage("Created {0:N0} BitSets in total (one per/Tag, per/QueryType, for {1:N0} Tags) - Using {2:N2} MB ({3:N2} GB) of memory\n",
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
                var bitSetsForQuery = GetTagBitSetForQueryType(queryType);
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
                tagsByAnswerCountBitSet[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByAnswerCountBitSet[tagToUse].Shrink();

                tagsByCreationDateBitSet[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByCreationDateBitSet[tagToUse].Shrink();

                tagsByLastActivityDateBitSet[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByLastActivityDateBitSet[tagToUse].Shrink();

                tagsByScoreBitSet[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByScoreBitSet[tagToUse].Shrink();

                tagsByViewCountBitSet[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                tagsByViewCountBitSet[tagToUse].Shrink();
            }
        }

        private string[] GetTagsToUseForBitSets(int minQuestionsPerTag)
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
            var validator = new Validator(questions, (format, args) => Logger.LogStartupMessage(format, args));
            var validationTimer = Stopwatch.StartNew();
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.LastActivityDate), (qu, prev) => qu.LastActivityDate <= prev.LastActivityDate);
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.CreationDate), (qu, prev) => Nullable.Compare<DateTime>(qu.CreationDate, prev.CreationDate) <= 0);
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.Score), (qu, prev) => Nullable.Compare(qu.Score, prev.Score) <= 0);
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.ViewCount), (qu, prev) => Nullable.Compare(qu.ViewCount, prev.ViewCount) <= 0);
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.AnswerCount), (qu, prev) => Nullable.Compare(qu.AnswerCount, prev.AnswerCount) <= 0);
            validationTimer.Stop();
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to VALIDATE all the {2:N0} arrays\n",
                  validationTimer.Elapsed, validationTimer.ElapsedMilliseconds, allTags.Count * 5);
        }

        private void ValidateBitSetOrdering()
        {
            // TODO Complete ValidateBitSetOrdering()
        }

        public class TagWithPositions
        {
            public int Count { get; set; }
            public int[] Positions { get; set; }
            public string Tag { get; set; }
        }
    }
}
