using ProtoBuf;
using Shared;
using StackOverflowTagServer.DataStructures;
using StackOverflowTagServer.Querying;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;
//using TagByQueryLookupBitSet = System.Collections.Generic.Dictionary<string, StackOverflowTagServer.DataStructures.AbstractBitSet>;
//using TagByQueryLookupBitSet = System.Collections.Generic.Dictionary<string, StackOverflowTagServer.DataStructures.IBitmapIndex>;
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

        private static readonly List<string> messages = new List<string>();
        public static List<string> Messages { get { return messages; } }

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
        public static TagServer CreateFromScratchAndSaveToDisk(List<Question> rawQuestions, string intermediateFilesFolder, bool useCompressedBitSets = true)
        {
            var tagServer = new TagServer(rawQuestions, useCompressedBitSets);
            var serializeTimer = Stopwatch.StartNew();
            Log("Serialisation folder: {0}", intermediateFilesFolder);

            foreach (QueryType type in (QueryType[])Enum.GetValues(typeof(QueryType)))
            {
                var tagLookupFileName = "intermediate-Lookup-" + type + ".bin";
                SerialiseToDisk(tagLookupFileName, intermediateFilesFolder, tagServer.GetTagLookupForQueryType(type));

                var bitSet = tagServer.GetTagBitSetForQueryType(type);
                if (bitSet.Count == 0)
                    continue;

                var timer = Stopwatch.StartNew();
                var bitMapIndexSerialiser = new Ewah.EwahCompressedBitArraySerializer();
                var bitMapIndexFileName = String.Format("intermediate-EWAH-BitMap-{0}.bin", type);
                var bitMapFilePath = Path.Combine(intermediateFilesFolder, bitMapIndexFileName);
                using (var fileSteam = new FileStream(bitMapFilePath, FileMode.Create))
                {
                    foreach (var item in bitSet)
                    {
                        // TODO we need a mechanism of packing all the <Tag, EwahCompressedBitArray> items into a single file
                        //var lengthOfEntireRecord = ?? // length of EwahCompressedBitArray + length of Tag/String (in bytes) + length of any other markers
                        //fileSteam.Write(BitConverter.GetBytes(lengthOfEntireRecord), 0, 4); // write length of the string (in bytes) out first

                        var tagAsBytes = Encoding.UTF8.GetBytes(item.Key);
                        fileSteam.Write(BitConverter.GetBytes(tagAsBytes.Length), 0, 4); // write length of the string (in bytes) out first
                        fileSteam.Write(tagAsBytes, 0, tagAsBytes.Length);

                        // We could write out the # of bits sets here?!, then use it as a sanity-check!?!?
                        fileSteam.Write(BitConverter.GetBytes(item.Value.GetCardinality()), 0, 8); // long is 64-bit, 8 byte

                        bitMapIndexSerialiser.Serialize(fileSteam, item.Value);
                    }
                }
                timer.Stop();
                var info = new FileInfo(bitMapFilePath);
                Log("Took {0} ({1,6:N0} ms) to serialise: {2} Size: {3,6:N2} MB",
                    timer.Elapsed, timer.ElapsedMilliseconds, bitMapIndexFileName.PadRight(50), info.Length / 1024.0 / 1024.0);

                //if (bitSet.Values.First() is BitmapIndex)
                //{
                //    var bitSetFileName = "intermediate-BitMap Index-" + type + ".bin";
                //    SerialiseToDisk(bitSetFileName, intermediateFilesFolder, bitSet);
                //    // TODO see if this can be done more efficiently, i.e. as a straight cast?!
                //    //SerialiseToDisk(bitSetFileName, intermediateFilesFolder, bitSet.ToDictionary(kvp => kvp.Key, kvp => kvp.Value as BitMap Index));
                //}
                //else if (bitSet.Values.First() is CompressedBitmapIndex)
                //{
                //    var bitSetFileName = "intermediate-CompressedBitSet-" + type + ".bin";
                //    SerialiseToDisk(bitSetFileName, intermediateFilesFolder, bitSet);
                //    // TODO see if this can be done more efficiently, i.e. as a straight cast?!
                //    //SerialiseToDisk(bitSetFileName, intermediateFilesFolder, bitSet.ToDictionary(kvp => kvp.Key, kvp => kvp.Value as CompressedBitSet));
                //}
            }

            // Now write out the AllTags Lookup, Tag -> Count (i.e. "C#" -> 579,321, "Java" -> 560,432)
            SerialiseToDisk(AllTagsFileName, intermediateFilesFolder, tagServer.AllTags);
            serializeTimer.Stop();
            Log("\nTook {0} (in TOTAL) to serialise the intermediate data TO disk\n", serializeTimer.Elapsed);

            return tagServer;
        }

        /// <summary>
        /// Private constructor that is used when creating the Tag Server from SCRATCH (<see cref="CreateFromScratchAndSaveToDisk"/>)
        /// </summary>
        /// <param name="questionsList"></param>
        private TagServer(List<Question> questionsList, bool useCompressedBitSets = true)
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

            Log(new string('#', Console.WindowWidth));
            if (useCompressedBitSets)
            {
                Log("Creating COMPRESSED BitSets");
                CreateBitSets(groupedTags, useCompressedBitSets: true);
            }
            else
            {
                Log("Creating regular BitSets");
                CreateBitSets(groupedTags);
            }
            Log(new string('#', Console.WindowWidth));

            //ValidateTagOrdering();
            //ValidateBitSetOrdering();

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Log("After TagServer created - Using {0:N2} MB ({1:N2} GB) of memory in total\n", mbUsed, mbUsed / 1024.0);
        }

        public static List<Question> GetRawQuestionsFromDisk(string folder, string filename)
        {
            List<Question> rawQuestions;
            var fileReadTimer = Stopwatch.StartNew();
            Console.WriteLine("DE-serialising the Stack Overflow Questions from the disk....");
            using (var file = File.OpenRead(Path.Combine(folder, filename)))
            {
                rawQuestions = Serializer.Deserialize<List<Question>>(file);
            }
            fileReadTimer.Stop();

            GC.Collect(2, GCCollectionMode.Forced);
            var memoryUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Log("Took {0} to DE-serialise {1:N0} Stack Overflow Questions from disk - Using {2:N2} MB ({3:N2} GB) of memory\n",
                fileReadTimer.Elapsed, rawQuestions.Count, memoryUsed, memoryUsed / 1024.0);

            return rawQuestions;
        }

        /// <summary>
        /// Factory method to create a <see cref="TagServer"/>, uses the private Constructor
        /// <see cref="TagServer(List{Question}, TagLookup, Dictionary{QueryType, TagByQueryLookup}, Dictionary{QueryType, TagByQueryLookupBitSet})"/>
        /// </summary>
        public static TagServer CreateFromSerialisedData(List<Question> rawQuestions, string intermediateFilesFolder,
                                                         bool deserialiseBitSets = true, bool useCompressedBitSets = true)
        {
            var deserializeTimer = Stopwatch.StartNew();
            Log("Deserialisation folder: {0}", intermediateFilesFolder);
            var queryTypes = (QueryType[])Enum.GetValues(typeof(QueryType));
            var intermediateLookups = new Dictionary<QueryType, TagByQueryLookup>(queryTypes.Length);
            var intermediateBitSets = new Dictionary<QueryType, TagByQueryLookupBitSet>(queryTypes.Length);
            foreach (QueryType type in queryTypes)
            {
                var tagLookupFileName = "intermediate-Lookup-" + type + ".bin";
                var tempLookup = DeserialiseFromDisk<TagByQueryLookup>(tagLookupFileName, intermediateFilesFolder);
                intermediateLookups.Add(type, tempLookup);
                Log("{0,20} contains {1:N0} Tag Lookups", type, tempLookup.Count);

                if (deserialiseBitSets)
                {
                    if (useCompressedBitSets)
                    {
                        var bitSetFileName = String.Format("intermediate-CompressedBitSet-{0}.bin", type);
                        //var tempBitSet = DeserialiseFromDisk<Dictionary<string, CompressedBitSet>>(bitSetFileName, intermediateFilesFolder);
                        //// TODO see if this can be done more efficiently, i.e. as a straight cast?!
                        //intermediateBitSets.Add(type, tempBitSet.ToDictionary(kvp => kvp.Key, kvp => kvp.Value as IBitSet));
                        var tempBitSet = DeserialiseFromDisk<TagByQueryLookupBitSet>(bitSetFileName, intermediateFilesFolder);
                        intermediateBitSets.Add(type, tempBitSet);
                        Log("{0,20} contains {1:N0} Tag BitSets", type, tempBitSet.Count);
                    }
                    else
                    {
                        var bitSetFileName = String.Format("intermediate-BitMap Index-{0}.bin", type);
                        //var tempBitSet = DeserialiseFromDisk<Dictionary<string, BitMap Index>>(bitSetFileName, intermediateFilesFolder);
                        //// TODO see if this can be done more efficiently, i.e. as a straight cast?!
                        //intermediateBitSets.Add(type, tempBitSet.ToDictionary(kvp => kvp.Key, kvp => kvp.Value as IBitSet));
                        var tempBitSet = DeserialiseFromDisk<TagByQueryLookupBitSet>(bitSetFileName, intermediateFilesFolder);
                        intermediateBitSets.Add(type, tempBitSet);
                        Log("{0,20} contains {1:N0} Tag BitSets", type, tempBitSet.Count);
                    }
                }
                else
                {
                    intermediateBitSets.Add(type, new TagByQueryLookupBitSet());
                }
            }
            // Now fetch from disk the AllTags Lookup, Tag -> Count (i.e. "C#" -> 579,321, "Java" -> 560,432)
            var allTags = DeserialiseFromDisk<TagLookup>(AllTagsFileName, intermediateFilesFolder);
            deserializeTimer.Stop();
            Log("\nTook {0} (in TOTAL) to DE-serialise the intermediate data FROM disk\n", deserializeTimer.Elapsed);

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
            Log("After TagServer created - Using {0:N2} MB ({1:N2} GB) of memory in total\n", mbUsed, mbUsed / 1024.0);
        }

        public int TotalCount(QueryType type, string tag)
        {
            TagByQueryLookup queryInfo = GetTagLookupForQueryType(type);
            return queryInfo[tag].Length;
        }

        internal static void TestBitSets(string intermediateFilesFolder)
        {
            var numItems = 8000000;
            var size = BitmapIndex.ToIntArrayLength(numItems);
            int numBitSetsToCreate = 100; // 1; // 4;

            var jumpsPerLoop = new[] { 10, 50, 100, 1000, 10000, 100000, 1000000 };
            Console.Write("\n" + new String('#', Console.WindowWidth));
            foreach (var jump in jumpsPerLoop)
            {
                var dictionaryBitSet = CreateBitSets(numBitSetsToCreate, size, numItems, jump);
                var dictionaryCompressedBitSet = CreateCompressedBitSets(numBitSetsToCreate, size, numItems, jump);

                SerialiseToDisk("BitMap Index-Normal.bin", intermediateFilesFolder, dictionaryBitSet);
                SerialiseToDisk("BitMap Index-Compressed.bin", intermediateFilesFolder, dictionaryCompressedBitSet);

                Console.Write("\n" + new String('#', Console.WindowWidth));
            }

            //var rtt = DeserialiseFromDisk<Dictionary<string, BitMap Index>>("BitMap Index-Testing.bin", intermediateFilesFolder);
            //var rttLengh = rtt["0"].InternalArray.Length;
            //var temp = fileSize + 1;
        }

        private static Dictionary<string, BitmapIndex> CreateBitSets(int numBitSetsToCreate, int size, int numItems, int jumpsPerLoop)
        {
            var memoryUsageBefore = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            var timer = Stopwatch.StartNew();
            var dictionaryBitSet = new Dictionary<string, BitmapIndex>();
            var bitsSet = 0;
            for (int i = 0; i < numBitSetsToCreate; i++)
            {
                var bitSet = new BitmapIndex(new int[size]);
                bitsSet = 0;
                for (int j = i; j < numItems; j += jumpsPerLoop)
                {
                    bitSet.MarkBit(j);
                    bitsSet++;
                    if (bitSet.IsMarked(j) == false)
                        Console.WriteLine("ERROR as posn[{0}] in BitMap Index[{1}]", j, i);
                }
                dictionaryBitSet.Add(i.ToString(), bitSet);
            }
            timer.Stop();
            Console.WriteLine("\nTook {0:N2} msecs to create {1} BitSets, each with {2:N0} individual Bit's Set",
                              timer.Elapsed.TotalMilliseconds, numBitSetsToCreate, bitsSet);

            var memoryUsageAfter = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Console.WriteLine("Using {0:N2} MB", memoryUsageAfter - memoryUsageBefore);
            var sizeofBitSet = (sizeof(int) * size) / 1024.0 / 1024.0;
            Console.WriteLine("Comparison {0:N2} MB (sizeof(int) = {1} bytes)", sizeofBitSet * numBitSetsToCreate, sizeof(int));

            return dictionaryBitSet;
        }

        private static Dictionary<string, CompressedBitmapIndex> CreateCompressedBitSets(int numBitSetsToCreate, int size, int numItems, int jumpsPerLoop)
        {
            var memoryUsageBefore = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            var timer = Stopwatch.StartNew();
            var dictionaryCompressedBitSet = new Dictionary<string, CompressedBitmapIndex>();
            var bitsSet = 0;
            for (int i = 0; i < numBitSetsToCreate; i++)
            {
                var compressedBitSet = new CompressedBitmapIndex(size, expectedFill: numItems / jumpsPerLoop);
                bitsSet = 0;
                for (int j = i; j < numItems; j += jumpsPerLoop)
                {
                    compressedBitSet.MarkBit(j);
                    bitsSet++;
                    if (compressedBitSet.IsMarked(j) == false)
                        Console.WriteLine("ERROR at posn[{0}] in Compressed BitMap Index[{1}]", j, i);
                }
                dictionaryCompressedBitSet.Add(i.ToString(), compressedBitSet);
            }
            timer.Stop();
            Console.WriteLine("\nTook {0:N2} msecs to create {1} Compressed BitSets, each with {2:N0} individual Bit's Set",
                              timer.Elapsed.TotalMilliseconds, numBitSetsToCreate, bitsSet);

            var memoryUsageAfter = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Console.WriteLine("Using {0:N2} MB", memoryUsageAfter - memoryUsageBefore);
            var sizeofBitSet = (sizeof(int) * size) / 1024.0 / 1024.0;
            Console.WriteLine("Comparison {0:N2} MB (sizeof(int) = {1} bytes)\n", sizeofBitSet * numBitSetsToCreate, sizeof(int));

            return dictionaryCompressedBitSet;
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

            ////Console.WriteLine("cSharp:        {0}",
            ////                  //String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => i.ToString(formatString))),
            ////                  String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => Convert.ToString(i, 2).PadLeft(32, '0'))));
            ////Console.WriteLine("jQuery:        {0}",
            ////                  //String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => i.ToString(formatString))),
            ////                  String.Join(spacer, jQuery.InternalArray.Skip(skipValue).Take(takeValue).Select(i => Convert.ToString(i, 2).PadLeft(32, '0'))));

            //jQuery.Not(); // Edits in-place, for real queries we NEED to make a copy
            //              //Console.WriteLine("NOT jQuery:    {0}",
            //              //                  //String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => i.ToString(formatString))),
            //              //                  String.Join(spacer, jQuery.InternalArray.Skip(skipValue).Take(takeValue).Select(i => Convert.ToString(i, 2).PadLeft(32, '0'))));

            //cSharp.Or(jQuery); // Edits in-place, for real queries we NEED to make a copy
            //                   //Console.WriteLine("cSharp OR (NOT jQuery)");

            ////cSharp.And(jQuery); // Edits in-place, for real queries we NEED to make a copy
            ////Console.WriteLine("cSharp AND (NOT jQuery)");

            ////Console.WriteLine("cSharp Result: {0}",
            ////                  //String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => i.ToString(formatString))),
            ////                  String.Join(spacer, cSharp.InternalArray.Skip(skipValue).Take(takeValue).Select(i => Convert.ToString(i, 2).PadLeft(32, '0'))));

            //bitSetTimer.Stop();

            //Log("Took {0} ({1:N0} ms) to do C# Or (Not jQuery)\n", bitSetTimer.Elapsed, bitSetTimer.ElapsedMilliseconds);
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
            Log("Took {0} ({1,6:N0} ms) to group all the tags - Using {2:N2} MB ({3:N2} GB) of memory\n",
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
            Log("Took {0} ({1,6:N0} ms) to sort the {2:N0} arrays {3}- Using {4:N2} MB ({5:N2} GB) of memory\n",
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

        private void CreateBitSets(Dictionary<string, TagWithPositions> groupedTags, bool useCompressedBitSets = false)
        {
            // First create all the BitSets we'll need, one per/Tag, per/QueryType
            var bitSetsTimer = Stopwatch.StartNew();
            var arraySize = BitmapIndex.ToIntArrayLength(questions.Count);
            var tagsToUse = GetTagsToUseForBitSets(minQuestionsPerTag: 0); // Don't use this for REGULAR BitSets, get an OOM Exception
            //var tagsToUse = GetTagsToUseForBitSets(minQuestionsPerTag: 500); // 3,975 Tags with MORE than 500 questions
            //var tagsToUse = GetTagsToUseForBitSets(minQuestionsPerTag: 1000); // 2,397 Tags with MORE than 1,000 questions
            //var tagsToUse = GetTagsToUseForBitSets(minQuestionsPerTag: 50000); // 48 Tags with MORE than 50,000 questions
            //var tagCounter = 0;
            foreach (var tagToUse in tagsToUse)
            {
                if (useCompressedBitSets)
                {
                    //tagsByAnswerCountBitSet.Add(tagToUse, new CompressedBitmapIndex(arraySize, expectedFill: TotalCount(QueryType.AnswerCount, tagToUse)));
                    //tagsByCreationDateBitSet.Add(tagToUse, new CompressedBitmapIndex(arraySize, expectedFill: TotalCount(QueryType.CreationDate, tagToUse)));
                    //tagsByLastActivityDateBitSet.Add(tagToUse, new CompressedBitmapIndex(arraySize, expectedFill: TotalCount(QueryType.LastActivityDate, tagToUse)));
                    //tagsByScoreBitSet.Add(tagToUse, new CompressedBitmapIndex(arraySize, expectedFill: TotalCount(QueryType.Score, tagToUse)));
                    //tagsByViewCountBitSet.Add(tagToUse, new CompressedBitmapIndex(arraySize, expectedFill: TotalCount(QueryType.ViewCount, tagToUse)));

                    tagsByAnswerCountBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
                    tagsByCreationDateBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
                    tagsByLastActivityDateBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
                    tagsByScoreBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
                    tagsByViewCountBitSet.Add(tagToUse, new Ewah.EwahCompressedBitArray());
                }
                else
                {
                    //tagsByAnswerCountBitSet.Add(tagToUse, new BitmapIndex(new int[arraySize]));
                    //tagsByCreationDateBitSet.Add(tagToUse, new BitmapIndex(new int[arraySize]));
                    //tagsByLastActivityDateBitSet.Add(tagToUse, new BitmapIndex(new int[arraySize]));
                    //tagsByScoreBitSet.Add(tagToUse, new BitmapIndex(new int[arraySize]));
                    //tagsByViewCountBitSet.Add(tagToUse, new BitmapIndex(new int[arraySize]));
                }

                //tagCounter++;
                //if (tagCounter % 1000 == 0 && tagCounter > 0)
                //    Log("Created BitSets for {0,8:N0} Tags ({1})", tagCounter, bitSetsTimer.Elapsed);
            }

            GC.Collect(2, GCCollectionMode.Forced);
            var mbUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Log("Created {0:N0} BitSets in total (one per/Tag, per/QueryType, for {1:N0} Tags) - Using {2:N2} MB ({3:N2} GB) of memory\n",
                tagsToUse.Length * 5, tagsToUse.Length, mbUsed, mbUsed / 1024.0);

            // Now populate the BitSets
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
                    if (question.Tags.Any(t => bitSetsForQuery.ContainsKey(t)) == false)
                        continue;

                    foreach (var tag in question.Tags.Where(t => bitSetsForQuery.ContainsKey(t)))
                    {
                        //bitSetsForQuery[tag].MarkBit(item.Index);
                        bitSetsForQuery[tag].Set(item.Index);

                        if (sanityCheck.ContainsKey(tag))
                            sanityCheck[tag]++;
                        else
                            sanityCheck.Add(tag, 1);
                    }
                }
                populationTimer.Stop();
                Log("Took {0} ({1,6:N0} ms) to populate BitMap Index for {2}", populationTimer.Elapsed, populationTimer.ElapsedMilliseconds, queryType);

                var sanityCheckTimer = Stopwatch.StartNew();
                foreach (var item in sanityCheck.OrderByDescending(t => t.Value))
                {
                    var firstError = true;
                    if (allTags[item.Key] != item.Value)
                    {
                        if (firstError)
                        {
                            Log("Errors in BitSets for {0}:", queryType);
                            firstError = false;
                        }

                        var errorText =
                            allTags[item.Key] != item.Value ?
                                string.Format(" *** Error expected {0}, but got {1} ***", allTags[item.Key], item.Value) : "";
                        Log("\t[{0}, {1:N0}]{2}", item.Key, item.Value, errorText);
                    }
                }
                sanityCheckTimer.Stop();
                Log("Took {0} ({1,6:N0} ms) to sanity-check BitMap Index for {2}", sanityCheckTimer.Elapsed, sanityCheckTimer.ElapsedMilliseconds, queryType);
            }
            bitSetsTimer.Stop();

            GC.Collect(2, GCCollectionMode.Forced);
            var memoryUsed = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Log("Took {0} ({1,6:N0} ms) to create the {2:N0} Bit Sets - Using {3:N2} MB ({4:N2} GB) of memory in total\n",
                bitSetsTimer.Elapsed, bitSetsTimer.ElapsedMilliseconds, tagsToUse.Length * 5, memoryUsed, memoryUsed / 1024.0);
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
            var validator = new Validator(questions, (format, args) => Log(format, args));
            var validationTimer = Stopwatch.StartNew();
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.LastActivityDate), (qu, prev) => qu.LastActivityDate <= prev.LastActivityDate);
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.CreationDate), (qu, prev) => Nullable.Compare<DateTime>(qu.CreationDate, prev.CreationDate) <= 0);
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.Score), (qu, prev) => Nullable.Compare(qu.Score, prev.Score) <= 0);
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.ViewCount), (qu, prev) => Nullable.Compare(qu.ViewCount, prev.ViewCount) <= 0);
            validator.ValidateTags(GetTagLookupForQueryType(QueryType.AnswerCount), (qu, prev) => Nullable.Compare(qu.AnswerCount, prev.AnswerCount) <= 0);
            validationTimer.Stop();
            Log("Took {0} ({1,6:N0} ms) to VALIDATE all the {2:N0} arrays\n",
                  validationTimer.Elapsed, validationTimer.ElapsedMilliseconds, allTags.Count * 5);
        }

        private void ValidateBitSetOrdering()
        {
            // TODO Complete ValidateBitSetOrdering()
        }

        private static void SerialiseToDisk<T>(string fileName, string folder, T item)
        {
            var filePath = Path.Combine(folder, fileName);
            var itemTimer = Stopwatch.StartNew();
            if (File.Exists(filePath))
                File.Delete(filePath);
            using (var file = File.OpenWrite(filePath))
            {
                Serializer.Serialize(file, item);
            }
            itemTimer.Stop();
            var info = new FileInfo(filePath);
            Log("Took {0} ({1,6:N0} ms) to serialise: {2} Size: {3,6:N2} MB",
                itemTimer.Elapsed, itemTimer.ElapsedMilliseconds, fileName.PadRight(50), info.Length / 1024.0 / 1024.0);
        }

        private static T DeserialiseFromDisk<T>(string fileName, string folder)
        {
            var filePath = Path.Combine(folder, fileName);
            var timer = Stopwatch.StartNew();
            T result = default(T);
            using (var file = File.OpenRead(filePath))
            {
                result = Serializer.Deserialize<T>(file);
            }
            timer.Stop();
            var info = new FileInfo(filePath);
            Log("Took {0} ({1,6:N0} ms) to DE-serialise: {2} Size: {3,6:N2} MB",
                timer.Elapsed, timer.ElapsedMilliseconds, fileName.PadRight(50), info.Length / 1024.0 / 1024.0);

            return result;
        }

        private static void Log(string format, params object[] args)
        {
            var msg = string.Format(format, args);
            Console.WriteLine(msg);
            Trace.WriteLine(msg);
            messages.Add(msg);
        }

        public class TagWithPositions
        {
            public int Count { get; set; }
            public int[] Positions { get; set; }
            public string Tag { get; set; }
        }
    }
}
