using ProtoBuf;
using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime;

using HashSet = StackOverflowTagServer.CLR.HashSet<string>;
//using HashSet = System.Collections.Generic.HashSet<string>;
using TagLookup = System.Collections.Generic.Dictionary<string, int>;
using NGrams = System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<int>>;

namespace StackOverflowTagServer
{
// ReSharper disable LocalizableElement
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("IsServerGC: {0}, LatencyMode: {1}", GCSettings.IsServerGC, GCSettings.LatencyMode);

            var folder = @"C:\Users\warma11\Downloads\__GitHub__\StackOverflowTagServer\BinaryData\";
            var filename = @"Questions-NEW.bin";
            //var filename = @"Questions-subset.bin";

            var startupTimer = Stopwatch.StartNew();
            var rawQuestions = GetRawQuestionsFromDisk(folder, filename);

            //TagServer tagServer = TagServer.CreateFromScratchAndSaveToDisk(rawQuestions, intermediateFilesFolder: folder);
            TagServer tagServer = TagServer.CreateFromSerialisedData(rawQuestions, intermediateFilesFolder: folder);

            //PrintQuestionStats(rawQuestions);
            //PrintTagStats(tagServer.AllTags);

            startupTimer.Stop();

            GC.Collect(2, GCCollectionMode.Forced);
            var totalMemory = GC.GetTotalMemory(true) / 1024.0 / 1024.0;
            Console.WriteLine("Took {0} ({1:N2} ms), in total to complete Startup - Using {2:N2} MB ({3:N2} GB) of memory in TOTAL", 
                              startupTimer.Elapsed, startupTimer.Elapsed.TotalMilliseconds, totalMemory, totalMemory / 1024.0);

            return;

            //RunComparisonQueries(tagServer);
            //return;

            Trie<int> trie = WildcardProcessor.CreateTrie(tagServer.AllTags);
            NGrams nGrams = WildcardProcessor.CreateNGrams(tagServer.AllTags, N: 3);

            // <.net> and <c#> aren't in this lists, so they can be valid tags!
            var leppieTags = GetLeppieTagsFromResource();
            var leppieExpandedTags = ProcessTagsForFastLookup(tagServer.AllTags, trie, nGrams, leppieTags);

            // Get some interesting stats on Leppie's Tag (how many qu's the cover/exclude, etc)
            GetLeppieTagInfo(rawQuestions, tagServer.AllTags, leppieTags, leppieExpandedTags);

            RunExclusionQueryTests(tagServer, leppieExpandedTags, runsPerLoop: 10);

            //RunSimpleQueries();

            Console.WriteLine("Finished, press <ENTER> to exit");
            Console.ReadLine();
        }     

        private static List<Question> GetRawQuestionsFromDisk(string folder, string filename)
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
            Console.WriteLine("Took {0} to DE-serialise {1:N0} Stack Overflow Questions from disk - Using {2:N2} MB ({3:N2} GB) of memory\n",
                                fileReadTimer.Elapsed, rawQuestions.Count, memoryUsed, memoryUsed / 1024.0);

            return rawQuestions;
        }

        private static void GetLeppieTagInfo(List<Question> rawQuestions, TagLookup allTags, List<string> leppieTags, HashSet leppieExpandedTags)
        {                       
            Console.WriteLine("\nThere are {0:N0} questions and {1:N0} tags in total", rawQuestions.Count, allTags.Count);
            Console.WriteLine("Leppie {0:N0} tags with wildcards expand to {1:N0} tags in total", leppieTags.Count, leppieExpandedTags.Count);
            var remainingTagsHashSet = new CLR.HashSet<string>(allTags.Keys);
            remainingTagsHashSet.ExceptWith(leppieExpandedTags);
            Console.WriteLine("There are {0:N0} tags remaining, {0:N0} + {1:N0} = {2:N0} (Expected: {3:N0})",
                              remainingTagsHashSet.Count, leppieExpandedTags.Count,
                              remainingTagsHashSet.Count + leppieExpandedTags.Count, allTags.Count);

            Console.WriteLine("Sanity checking excluded/included tags and questions...");
            var excludedQuestionCounter = rawQuestions.Count(question => question.Tags.Any(t => leppieExpandedTags.Contains(t)));
            var includedQuestionCounter = rawQuestions.Count(question => question.Tags.All(t => remainingTagsHashSet.Contains(t)));
            Console.WriteLine("{0:N0} EXCLUDED tags cover {1:N0} questions (out of {2:N0})", 
                              leppieExpandedTags.Count, excludedQuestionCounter, rawQuestions.Count);
            Console.WriteLine(
                "{0:N0} remaining tags cover {1:N0} questions, {2:N0} + {3:N0} = {4:N0} (Expected: {5:N0})",
                remainingTagsHashSet.Count, includedQuestionCounter,
                includedQuestionCounter, excludedQuestionCounter,
                includedQuestionCounter + excludedQuestionCounter, rawQuestions.Count);
            Console.WriteLine();
        }

        private static List<string> GetLeppieTagsFromResource()
        {
            var leppieTags = new List<string>();
            var resourceStream = GetStream("leppie - excluded tags.txt");
            if (resourceStream != null)
            {
                var fileStream = new StreamReader(resourceStream);
                string line;
                while ((line = fileStream.ReadLine()) != null)
                    leppieTags.Add(line);
                //Console.WriteLine(string.Join(", ", tagsToExpand));
            }
            return leppieTags;
        }

        private static HashSet ProcessTagsForFastLookup(TagLookup allTags, Trie<int> trie, NGrams nGrams, List<string> tagsToExpand)
        {
            var expandTagsContainsTimer = Stopwatch.StartNew();
            var expandTagsContains = WildcardProcessor.ExpandTagsContainsStartsWithEndsWith(allTags, tagsToExpand);
            expandTagsContainsTimer.Stop();

            var expandTagsVBTimer = Stopwatch.StartNew();
            var expandedTagsVB = WildcardProcessor.ExpandTagsVisualBasic(allTags, tagsToExpand);
            expandTagsVBTimer.Stop();

            var expandTagsRegexTimer = Stopwatch.StartNew();
            var expandedTagsRegex = WildcardProcessor.ExpandTagsRegex(allTags, tagsToExpand);
            expandTagsRegexTimer.Stop();

            var expandTagsTrieTimer = Stopwatch.StartNew();
            var expandedTagsTrie = WildcardProcessor.ExpandTagsTrie(allTags, tagsToExpand, trie);
            expandTagsTrieTimer.Stop();

            var expandedTagsNGramsTimer = Stopwatch.StartNew();
            var expandedTagsNGrams = WildcardProcessor.ExpandTagsNGrams(allTags, tagsToExpand, nGrams);
            expandTagsRegexTimer.Stop();

            Console.WriteLine("There are {0:N0} tags in total", allTags.Count);
            Console.WriteLine("There are {0:N0} tags (raw) BEFORE expansion", tagsToExpand.Count);
            Console.WriteLine("Expanded to {0:N0} tags (Contains),  took {1,8:N2} ms ({2})",
                  expandTagsContains.Count, expandTagsContainsTimer.Elapsed.TotalMilliseconds, expandTagsContainsTimer.Elapsed);
            Console.WriteLine("Expanded to {0:N0} tags (VB),        took {1,8:N2} ms ({2})",
                            expandedTagsVB.Count, expandTagsVBTimer.Elapsed.TotalMilliseconds, expandTagsVBTimer.Elapsed);
            Console.WriteLine("Expanded to {0:N0} tags (Regex),     took {1,8:N2} ms ({2})",
                            expandedTagsRegex.Count, expandTagsRegexTimer.Elapsed.TotalMilliseconds, expandTagsRegexTimer.Elapsed);
            Console.WriteLine("Expanded to {0:N0} tags (Trie),      took {1,8:N2} ms ({2})",
                            expandedTagsTrie.Count, expandTagsTrieTimer.Elapsed.TotalMilliseconds, expandTagsTrieTimer.Elapsed);
            Console.WriteLine("Expanded to {0:N0} tags (NGrams),    took {1,8:N2} ms ({2})",
                            expandedTagsNGrams.Count, expandedTagsNGramsTimer.Elapsed.TotalMilliseconds, expandedTagsNGramsTimer.Elapsed);

            Console.WriteLine("\nIn Contains but not in Regex: " + string.Join(", ", expandTagsContains.Except(expandedTagsRegex)));
            Console.WriteLine("\nIn Regex but not in Contains: " + string.Join(", ", expandedTagsRegex.Except(expandTagsContains)));

            Console.WriteLine("\nIn Contains but not in VB: " + string.Join(", ", expandTagsContains.Except(expandedTagsVB)));
            Console.WriteLine("\nIn VB but not in Contains: " + string.Join(", ", expandedTagsVB.Except(expandTagsContains)));

            Console.WriteLine("\nIn Contains but not in Trie: " + string.Join(", ", expandTagsContains.Except(expandedTagsTrie)));
            Console.WriteLine("\nIn Trie but not in Contains: " + string.Join(", ", expandedTagsTrie.Except(expandTagsContains)));

            Console.WriteLine("\nIn Contains but not in NGrams: " + string.Join(", ", expandTagsContains.Except(expandedTagsNGrams)));
            Console.WriteLine("\nIn NGrams but not in Contains: " + string.Join(", ", expandedTagsNGrams.Except(expandTagsContains)));
            Console.WriteLine();

            var expandedTags = expandedTagsNGrams;
            //Console.WriteLine(string.Join(", ", expandedTags));

            // This is an error, we shouldn't have extra tags that aren't in the "allTags" list!!
            var extra = expandedTags.Except(allTags.Keys).ToList();
            if (extra.Count > 0)
                Console.WriteLine("\nExtra Tags: " + string.Join(", ", extra) + "\n");

            return expandedTags;
        }

        private static void RunExclusionQueryTests(TagServer tagServer, HashSet expandedTags, int runsPerLoop)
        {
            Results.CreateNewFile(string.Format("Results-Exclusion-Queries-{0}.csv", DateTime.Now.ToString("yyyy-MM-dd @ HH-mm-ss")));
            //Results.AddHeaders("Count", "Slow", "Fast", "Fast Alt"); // "Bloom"
            Results.AddHeaders("Count", "Fast", "Fast Alt");

            var amounts = new List<int>();
            for (decimal notQueries = (25.0m / 16.0m); notQueries <= 6400; notQueries *= 2)
            {
                amounts.Add((int)notQueries);
            }
            amounts.Add(expandedTags.Count);

            Console.WriteLine("\n\nVarying the number of \"Not Queries\"\n");
            var expandedTagsAsList = expandedTags.ToList();
            //while (true)
            {
                foreach (var count in amounts)
                {
                    var pageSize = 50;
                    var excludedTags = SelectNItemsFromList(expandedTagsAsList, count);
                    //Console.WriteLine("Count={0}, excludedTags.Count={1} tags:{2}\n\n", count, excludedTags.Count, string.Join(", ", excludedTags));
                    //continue;

                    //GC.Collect(2, GCCollectionMode.Forced);
                    for (int i = 0; i < runsPerLoop; i++)
                    {
                        Results.AddData(excludedTags.Count.ToString());

                        //var results1 = tagServer.BooleanQueryWithExclusionsLINQVersion(QueryType.Score, ".net", excludedTags, pageSize: pageSize);
                        var results2 = tagServer.BooleanQueryWithExclusionsFastVersion(QueryType.Score, ".net", excludedTags, pageSize: pageSize);
                        var results3 = tagServer.BooleanQueryWithExclusionsFastAlternativeVersion(QueryType.Score, ".net", excludedTags, pageSize: pageSize);
                        //var results4 = tagServer.BooleanQueryWithExclusionsBloomFilterVersion(QueryType.Score, ".net", excludedTags, pageSize: pageSize);

                        //CompareLists(listA: results1, nameA: "Slow", listB: results2, nameB: "Fast");
                        CompareLists(listA: results2, nameA: "Fast", listB: results3, nameB: "FastAlternative");
                        //CompareLists(listA: results3, nameA: "FastAlternative", listB: results4, nameB: "Bloom");

                        Results.StartNewRow();
                    }
                    //GC.Collect(2, GCCollectionMode.Forced);
                }
            }

            //Console.WriteLine("\n\nVarying the number of \"Skipped Pages\"\n");
            ////for (decimal skip = (25.0m / 16.0m); skip <= 1600; skip *= 2)
            //for (decimal skip = (25.0m / 16.0m); skip <= 800; skip *= 2)
            //{
            //    var pageSize = 50;
            //    for (int i = 0; i < runsPerLoop; i++)
            //    {
            //        var resultsSlow = tagServer.BooleanQueryWithExclusionsLINQVersion(QueryType.Score, ".net", notQueries: 400, pageSize: pageSize, skip: (int)skip);
            //        var resultsFast = tagServer.BooleanQueryWithExclusionsFastVersion(QueryType.Score, ".net", notQueries: 400, pageSize: pageSize, skip: (int)skip);
            //        var resultsFastAlt = tagServer.BooleanQueryWithExclusionsFastAlternativeVersion(QueryType.Score, ".net", notQueries: 400, pageSize: pageSize, skip: (int)skip);
            //        //tagServer.BooleanQueryWithExclusionsBloomFilterVersion(QueryType.Score, ".net", notQueries: 400, pageSize: pageSize, skip: (int)skip);
            //    }
            //}

            //return;

            Results.CloseFile();
        }

        private static void RunComparisonQueries(TagServer tagServer)
        {
            var smallTag = tagServer.AllTags.Where(t => t.Value <= 200).First().Key;
            string largeTag = ".net";
            int pageSize = 25;

            // LARGE 1st Tag, SMALL 2nd Tag
            //RunAndOrNotComparisionQueries(tagServer, tag1: largeTag, tag2: smallTag, pageSize: pageSize);
            // SMALL 1st Tag, LARGE 2nd Tag
            //RunAndOrNotComparisionQueries(tagServer, tag1: smallTag, tag2: largeTag, pageSize: pageSize);

            // 2 large tags (probably the worst case)
            RunAndOrNotComparisionQueries(tagServer, "c#", "jquery", pageSize);
        }

        private static void RunAndOrNotComparisionQueries(TagServer tagServer, string tag1, string tag2, int pageSize)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("\nComparison queries:\n\t\"{0}\" has {1:N0} questions\n\t\"{2}\" has {3:N0} questions",
                              tag1, tagServer.AllTags[tag1], tag2, tagServer.AllTags[tag2]);
            Console.ResetColor();

            var queries = new[] { "AND", "OR", "NOT", "OR-NOT" };
            //var queries = new[] { "OR-NOT" };

            var skipCounts = new[] { 0, 100, 250, 500, 1000, 2000, 4000, 8000 };
            foreach (var query in queries)
            {
                Results.CreateNewFile(string.Format("Results-{0}-{1}-{2}-{3}.csv", DateTime.Now.ToString("yyyy-MM-dd @ HH-mm-ss"), tag1, query, tag2));
                Results.AddHeaders("Skip Count", "Regular", "Regular", "Regular", "No LINQ", "No LINQ", "No LINQ");

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("\n{0} Comparison queries: {1} {0} {2}\n", query, tag1, tag2);
                Console.ResetColor();
                foreach (var skipCount in skipCounts)
                {
                    Results.AddData(skipCount.ToString());
                    Console.ForegroundColor = ConsoleColor.DarkGreen;
                    var result1 = tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, query, pageSize: pageSize, skip: skipCount);
                    var result2 = tagServer.ComparisonQuery(QueryType.Score, tag2, tag1, query, pageSize: pageSize, skip: skipCount);
                    var result3 = tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, query, pageSize: pageSize, skip: skipCount);

                    Console.ForegroundColor = ConsoleColor.Cyan;
                    var result4 = tagServer.ComparisonQueryNoLINQ(QueryType.Score, tag1, tag2, query, pageSize: pageSize, skip: skipCount);
                    var result5 = tagServer.ComparisonQueryNoLINQ(QueryType.Score, tag2, tag1, query, pageSize: pageSize, skip: skipCount);
                    var result6 = tagServer.ComparisonQueryNoLINQ(QueryType.Score, tag1, tag2, query, pageSize: pageSize, skip: skipCount);

                    CompareLists(result1, "Regular", result4, "No LINQ");
                    CompareLists(result2, "Regular", result5, "No LINQ");
                    CompareLists(result3, "Regular", result6, "No LINQ");

                    //Console.ForegroundColor = ConsoleColor.DarkYellow;
                    //var result7 = tagServer.ComparisonQueryAdv(QueryType.Score, tag1, tag2, query, pageSize: pageSize, skip: skipCount);
                    //var result8 = tagServer.ComparisonQueryAdv(QueryType.Score, tag2, tag1, query, pageSize: pageSize, skip: skipCount);
                    //var result9 = tagServer.ComparisonQueryAdv(QueryType.Score, tag1, tag2, query, pageSize: pageSize, skip: skipCount);

                    Console.ResetColor();

                    Results.StartNewRow();
                }

                Results.CloseFile();
            }
        }

        private static void RunSimpleQueries()
        {
            //var queryTester = new QueryTester(tagServer.Questions);
            //queryTester.TestAndOrNotQueries();
            //queryTester.TestQueries();

            //Console.WriteLine("Finished, press <ENTER> to exit");
            //Console.ReadLine();
            //return;

            // Regular queries, i.e. single tag, no Boolean operators
            //tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 10, skip: 0);
            //tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 10, skip: 9);
            //tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 10, skip: 10);

            //tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 100, skip: 10000);
            //tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 100, skip: 1000000);

            //tagServer.Query(QueryType.Score, ".net", pageSize: 6, skip: 95);
            //tagServer.Query(QueryType.Score, ".net", pageSize: 6, skip: 100);
            //tagServer.Query(QueryType.Score, ".net", pageSize: 6, skip: 105);
        }

        private static void PrintQuestionStats(List<Question> rawQuestions)
        {
            // For sanity checks!!
            Console.WriteLine("Max LastActivityDate {0}", rawQuestions.Max(q => q.LastActivityDate));
            Console.WriteLine("Min LastActivityDate {0}\n", rawQuestions.Min(q => q.LastActivityDate));

            Console.WriteLine("Max CreationDate {0}", rawQuestions.Max(q => q.CreationDate));
            Console.WriteLine("Min CreationDate {0}\n", rawQuestions.Min(q => q.CreationDate));

            Console.WriteLine("Max  Score {0}", rawQuestions.Max(q => q.Score));
            Console.WriteLine("Min  Score {0}", rawQuestions.Min(q => q.Score));
            Console.WriteLine("Null Score {0}\n", rawQuestions.Count(q => q.Score == null));

            Console.WriteLine("Max  ViewCount {0}", rawQuestions.Max(q => q.ViewCount));
            Console.WriteLine("Min  ViewCount {0}", rawQuestions.Min(q => q.ViewCount));
            Console.WriteLine("Null ViewCount {0}\n", rawQuestions.Count(q => q.ViewCount == null));

            Console.WriteLine("Max  AnswerCount {0}", rawQuestions.Max(q => q.AnswerCount));
            Console.WriteLine("Min  AnswerCount {0}", rawQuestions.Min(q => q.AnswerCount));
            Console.WriteLine("Null AnswerCount {0}\n", rawQuestions.Count(q => q.AnswerCount == null));

            var mostViewed = rawQuestions.OrderByDescending(q => q.ViewCount).First();
            var mostAnswers = rawQuestions.OrderByDescending(q => q.AnswerCount).First();
            var highestScore = rawQuestions.OrderByDescending(q => q.Score).First();
            Console.WriteLine("Max ViewCount {0}, Question Id {1}", mostViewed.ViewCount, mostViewed.Id);
            Console.WriteLine("Max Answers {0}, Question Id {1}", mostAnswers.AnswerCount, mostAnswers.Id);
            Console.WriteLine("Max Score {0}, Question Id {1}\n", highestScore.Score, highestScore.Id);            
        }

        private static void PrintTagStats(TagLookup allTags)
        {
            var histogram = new Dictionary<int, List<KeyValuePair<string, int>>>();
            foreach (var tag in allTags)
            {
                if (tag.Key == TagServer.ALL_TAGS_KEY)
                    continue;

                var bucket = -1;
                if (tag.Value <= 10)
                    bucket = tag.Value;
                else if (tag.Value < 100)
                    bucket = ((tag.Value / 10) * 10) + 10;
                else if (tag.Value < 1000)
                    bucket = ((tag.Value / 100) * 100) + 100;
                else if (tag.Value < 10000)
                    bucket = ((tag.Value / 1000) * 1000) + 1000;
                else if (tag.Value < 100000)
                    bucket = ((tag.Value / 10000) * 10000) + 10000;
                else if (tag.Value < 1000000)
                    bucket = ((tag.Value / 100000) * 100000) + 100000;
                else
                    bucket = tag.Value;

                if (bucket == -1)
                {
                    Console.WriteLine("Error: ({0}, {1})", tag.Key, tag.Value);
                    continue;
                }

                if (histogram.ContainsKey(bucket))
                {
                    var bucketInfo = histogram[bucket];
                    bucketInfo.Add(tag);
                }
                else
                {
                    var list = new List<KeyValuePair<string, int>> { tag };
                    histogram.Add(bucket, list);
                }
            }

            var tagCountCutoff = 25000; // 50000;
            var topTags = allTags.Where(t => t.Value > tagCountCutoff && 
                                             t.Key != TagServer.ALL_TAGS_KEY)
                                 .Select(t => "\"" + t.Key + "\"")
                                 .ToList();
            Console.WriteLine("Tags with MORE than {0:N0} questions ({1} in total): ", tagCountCutoff, topTags.Count);
            Console.WriteLine("{{ {0} }}", String.Join(", ", topTags));

            Console.WriteLine();
            foreach (var bucket in histogram.OrderByDescending(h => h.Key))
            {
                Console.WriteLine("{0,8:N0}: {1:N0}", bucket.Key, bucket.Value.Count);
            }

            //Console.WriteLine();
            //foreach (var bucket in histogram.OrderByDescending(h => h.Key))
            //{
            //    if (bucket.Key > 10000)
            //        Console.WriteLine("{0,8:N0}: {1:N0} -> {2}\n", 
            //            bucket.Key, bucket.Value.Count, 
            //            String.Join(", ", bucket.Value.Select(b => string.Format("[{0:N0}, {1:N0}]", b.Key, b.Value))));
            //}
        }

#region HelperMethods
        private static void CompareLists(List<Question> listA, string nameA, List<Question> listB, string nameB)
        {
            if (listA.Count != listB.Count)
                Console.WriteLine("ERROR: list have different lengths, {0}: {1}, {2}: {3}", nameA, listA.Count, nameB, listB.Count);
            var AExceptB = listA.Select(r => r.Id).Except(listB.Select(r => r.Id)).ToList();
            if (AExceptB.Any())
            {
                Console.WriteLine("ERROR: Items in {0}, but not in {1}: {2}\n", nameA, nameB,
                                  string.Join(", ", AExceptB.Select(r => string.Format("[{0}]={1}", listA.FindIndex(s => s.Id == r), r))));
            }
            var BExceptA = listB.Select(r => r.Id).Except(listA.Select(r => r.Id)).ToList();
            if (BExceptA.Any())
            {
                Console.WriteLine("ERROR: Items in {0}, but not in {1}: {2}\n", nameB, nameA,
                                  string.Join(", ", BExceptA.Select(r => string.Format("[{0}]={1}", listB.FindIndex(s => s.Id == r), r))));
            }

            //foreach (var item in Enumerable.Range(0, Math.Min(listA.Count, listB.Count)))
            //{
            //    if (listA[item].Id != listB[item].Id)
            //        Console.WriteLine("ERROR: lists differ at position[{0}], {1} Id: {2}, {3} Id: {4}", 
            //                          item, nameA, listA[item].Id, nameB, listB[item].Id);
            //}
        }

        private static List<string> SelectNItemsFromList(List<string> expandedTags, int count)
        {
            if (count == expandedTags.Count)
                return expandedTags;
            else
            {
                var result = new List<string>(count);
                var step = expandedTags.Count / count;
                int i = 0;
                while (i < expandedTags.Count && (result.Count < count))
                {
                    result.Add(expandedTags[i]);
                    i += step;
                }
                return result;
            }
        }

        // From http://stackoverflow.com/questions/11590582/read-text-file-resource-from-net-library/11596483#11596483
        private static Stream GetStream(string resourceName)
        {
            try
            {
                Assembly assy = Assembly.GetExecutingAssembly();
                string[] resources = assy.GetManifestResourceNames();
                for (int i = 0; i < resources.Length; i++)
                {
                    if (resources[i].ToLower().IndexOf(resourceName.ToLower()) != -1)
                    {
                        // resource found
                        return assy.GetManifestResourceStream(resources[i]);
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
            return Stream.Null;
        }
#endregion #region HelperMethods
    }
    // ReSharper restore LocalizableElement
}
