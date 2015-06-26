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
            //var filename = @"Questions.bin";
            var filename = @"Questions-NEW.bin";
            //var filename = @"Questions-subset.bin";

            //var allTagsFilename = "intermediate-AllTags.bin";
            //TagLookup allTags;
            //using (var file = File.OpenRead(Path.Combine(folder, allTagsFilename)))
            //{
            //    allTags = Serializer.Deserialize<TagLookup>(file);
            //}
            
            //var nGrams = WildcardProcessor.CreateNGramsForIndexing(allTags.Keys, N: 3);

            // <.net> and <c#> aren't in this lists, so they can be valid tags!            
            var leppieTags = GetLeppieTagsFromResource();

            //TestWithNGrams(allTags, nGrams, leppieTags);

            var startupTimer = Stopwatch.StartNew();
            List<Question> rawQuestions;
            var memoryBefore = GC.GetTotalMemory(true);
            var fileReadTimer = Stopwatch.StartNew();
            Console.WriteLine("DE-serialising the Stack Overflow Questions from the disk....");
            using (var file = File.OpenRead(Path.Combine(folder, filename)))
            {
                rawQuestions = Serializer.Deserialize<List<Question>>(file);
            }
            fileReadTimer.Stop();
            var memoryAfter = GC.GetTotalMemory(true);
            Console.WriteLine("Took {0} to DE-serialise {1:N0} Stack Overflow Questions from the file, used {2:0.00} MB of memory\n",
                                fileReadTimer.Elapsed, rawQuestions.Count, (memoryAfter - memoryBefore) / 1024.0 / 1024.0);
           
            //TagServer tagServer = CreateTagServer(rawQuestions, createFromScratch: true, intermediateFilesFolder: folder);
            TagServer tagServer = CreateTagServer(rawQuestions, createFromScratch: false, intermediateFilesFolder: folder);

            startupTimer.Stop();
            Console.WriteLine("Took {0} (in total) to complete Startup\n", startupTimer.Elapsed);
            
            Trie<int> trie = WildcardProcessor.CreateTrie(tagServer.AllTags);            
            NGrams nGrams = WildcardProcessor.CreateNGrams(tagServer.AllTags.Keys, N: 3);            

            var leppieExpandedTags = ProcessTagsForFastLookup(tagServer.AllTags, trie, nGrams, leppieTags);

            // Get some interesting stats on Leppie's Tag (how many qu's the cover/exclude, etc)
            GetLeppieTagInfo(rawQuestions, tagServer.AllTags, leppieTags, leppieExpandedTags);

            //List<string> leppieExpandedTags = ProcessTagsForFastLookup(tagServer.AllTags, trie, tagsToExpand: leppieTags);
            //TestTagWildcardExpansion(tagServer.AllTags, trie); 

            //TestWithNGrams(tagServer.AllTags, leppieTags);

            return;

#region QueryTestingCode
            //var queryTester = new QueryTester(tagServer.Questions);
            //queryTester.TestAndOrNotQueries();
            //queryTester.TestQueries();

            //Console.WriteLine("Finished, press <ENTER> to exit");
            //Console.ReadLine();
            //return;

            //if (true)
            if (false)
            {
                RunComparisonQueries(tagServer);
                return;
            }

            int runsPerLoop = 10;
            //int runsPerLoop = 1;

            RunExclusionQueryTests(tagServer, leppieExpandedTags, runsPerLoop);

            //Console.WriteLine("\n\nVarying the number of \"Skipped Pages\"\n");
            ////for (decimal skip = (25.0m / 16.0m); skip <= 1600; skip *= 2)
            //for (decimal skip = (25.0m / 16.0m); skip <= 800; skip *= 2)
            //{
            //    var pageSize = 50;
            //    for (int i = 0; i < runsPerLoop; i++)
            //    {
            //        var resultsSlow = tagServer.BooleanQueryWithExclusionsSlowVersion(QueryType.Score, ".net", notQueries: 400, pageSize: pageSize, skip: (int)skip);
            //        var resultsFast = tagServer.BooleanQueryWithExclusionsFastVersion(QueryType.Score, ".net", notQueries: 400, pageSize: pageSize, skip: (int)skip);
            //        var resultsFastAlt = tagServer.BooleanQueryWithExclusionsFastAlternativeVersion(QueryType.Score, ".net", notQueries: 400, pageSize: pageSize, skip: (int)skip);
            //        //tagServer.BooleanQueryWithExclusionsBloomFilterVersion(QueryType.Score, ".net", notQueries: 400, pageSize: pageSize, skip: (int)skip);
            //    }
            //}

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

            Console.WriteLine("Finished, press <ENTER> to exit");
            Console.ReadLine();
#endregion
        }

        private static void TestWithNGrams(TagLookup allTags, NGrams nGrams, List<string> tagsToExpand)
        {
            //var sampleTags = new List<string>(new[]
            //    {
            //        ".net", "c#", "c#-4.0", "c#-5.0", "java", "nhibernate", "hibernate", "java-8", "java-7"
            //    });
            //var ngrams = WildcardProcessor.CreateNGramsForIndexing(sampleTags, N: 3);
            //Console.WriteLine("Tri-grams (n-grams):");
            //foreach (var ngram in ngrams)
            //{
            //    Console.WriteLine("\t{0}: {1}", ngram.Key, String.Join(", ", ngram.Value.Select(i => sampleTags[i])));
            //}
            //return;

            //Console.WriteLine("Tri-grams (n-grams):");
            //foreach (var ngram in nGrams)
            //{
            //    Console.WriteLine("\t{0}: {1}", ngram.Key, String.Join(", ", ngram.Value.Select(i => tagServer.AllTags.ElementAt(i))));
            //}
            //Console.WriteLine("Took {0} to create {1} n-grams\n", nGrams.Count, nGramsTimer.Elapsed);

            //var leppieExpandTimer = Stopwatch.StartNew();
            //var allTagsList = allTags.Keys.ToList();
            //int tagsExpandedCounter;
            //var allExpandedTags = WildcardProcessor.ExpandTagsNGrams(allTags: allTagsList, tagsToExpand: tagsToExpand, nGrams: nGrams);
            //leppieExpandTimer.Stop();
            //Console.WriteLine("\nTook {0} ({1,5:N0} ms) to expand {2} items (out of {3}) to {4} Tags\n", leppieExpandTimer.Elapsed,
            //                  leppieExpandTimer.ElapsedMilliseconds, tagsExpandedCounter, tagsToExpand.Count, allExpandedTags.Count);
        }

        private static void GetLeppieTagInfo(List<Question> rawQuestions, TagLookup allTags, List<string> leppieTags, List<string> leppieExpandedTags)
        {                       
            Console.WriteLine("\nThere are {0:N0} questions and {1:N0} tags in total", rawQuestions.Count, allTags.Count);
            Console.WriteLine("Leppie {0:N0} tags with wildcards expand to {1:N0} tags in total", leppieTags.Count, leppieExpandedTags.Count);
            var expandedTagsHashSet = new CLR.HashSet<string>(leppieExpandedTags);
            var remainingTagsHashSet = new CLR.HashSet<string>(allTags.Keys);
            remainingTagsHashSet.ExceptWith(leppieExpandedTags);
            Console.WriteLine("There are {0:N0} tags remaining ({0:N0} + {1:N0} = {2:N0} (Expected: {3:N0}))",
                              remainingTagsHashSet.Count, expandedTagsHashSet.Count,
                              remainingTagsHashSet.Count + expandedTagsHashSet.Count, allTags.Count);

            Console.WriteLine("Sanity checking excluded/included tags and questions...");
            var excludedQuestionCounter = 0;
            foreach (var question in rawQuestions)
            {
                if (question.Tags.Any(t => expandedTagsHashSet.Contains(t)))
                    excludedQuestionCounter++;
            }

            var includedQuestionCounter = 0;
            foreach (var question in rawQuestions)
            {
                if (question.Tags.All(t => remainingTagsHashSet.Contains(t)))
                    includedQuestionCounter++;
            }
            Console.WriteLine("{0:N0} EXCLUDED tags cover {1:N0} questions (out of {2:N0})", 
                              leppieExpandedTags.Count, excludedQuestionCounter, rawQuestions.Count);
            Console.WriteLine(
                "{0:N0} remaining tags cover {1:N0} questions ({2:N0} + {3:N0} = {4:N0} (Expected: {5:N0}))",
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

        private static List<string> ProcessTagsForFastLookup(TagLookup allTags, Trie<int> trie, NGrams nGrams, List<string> tagsToExpand)
        {            
            Console.WriteLine("\nThere are {0:N0} tags in total", allTags.Count);

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

            Console.WriteLine("There are {0:N0} tags (raw) BEFORE expansion", tagsToExpand.Count);
            Console.WriteLine("Expanded to {0:N0} tags (VB),     took {1,8:N2} ms ({2})",
                            expandedTagsVB.Count, expandTagsVBTimer.Elapsed.TotalMilliseconds, expandTagsVBTimer.Elapsed);
            Console.WriteLine("Expanded to {0:N0} tags (Regex),  took {1,8:N2} ms ({2})",
                            expandedTagsRegex.Count, expandTagsRegexTimer.Elapsed.TotalMilliseconds, expandTagsRegexTimer.Elapsed);
            Console.WriteLine("Expanded to {0:N0} tags (Trie),   took {1,8:N2} ms ({2})",
                            expandedTagsTrie.Count, expandTagsTrieTimer.Elapsed.TotalMilliseconds, expandTagsTrieTimer.Elapsed);
            Console.WriteLine("Expanded to {0:N0} tags (NGrams), took {1,8:N2} ms ({2})",
                            expandedTagsNGrams.Count, expandedTagsNGramsTimer.Elapsed.TotalMilliseconds, expandedTagsNGramsTimer.Elapsed);

            Console.WriteLine("\nIn Regex but not in VB: " + string.Join(", ", expandedTagsRegex.Except(expandedTagsVB)));
            Console.WriteLine("\nIn VB but not in Regex: " + string.Join(", ", expandedTagsVB.Except(expandedTagsRegex)));

            Console.WriteLine("\nIn Regex but not in Trie: " + string.Join(", ", expandedTagsRegex.Except(expandedTagsTrie)));
            Console.WriteLine("\nIn Trie but not in Regex: " + string.Join(", ", expandedTagsTrie.Except(expandedTagsRegex)));

            Console.WriteLine("\nIn Regex but not in NGrams: " + string.Join(", ", expandedTagsRegex.Except(expandedTagsNGrams)));
            Console.WriteLine("\nIn NGrams but not in Regex: " + string.Join(", ", expandedTagsNGrams.Except(expandedTagsRegex)));
            Console.WriteLine();            

            var expandedTags = expandedTagsTrie;
            //Console.WriteLine(string.Join(", ", expandedTags));

            // This is an error, we should've have extra tags that aren't in the "allTags" list!!
            var extra = expandedTags.Except(allTags.Keys).ToList();
            if (extra.Count > 0)
                Console.WriteLine("\nExtra Tags: " + string.Join(", ", extra) + "\n");            

            return expandedTags;
        }

        private static void TestTagWildcardExpansion(TagLookup allTags, Trie<int> trie)
        {
            var test = new[] {"*js"}.ToList();
            var results = WildcardProcessor.ExpandTagsTrie(allTags, test, trie);
            Console.WriteLine("Testing *js Contains(\"json\") = {0} :\n{1}", results.Contains("json"), string.Join(", ", results));

            var tests = new[]
                {
                    "actionscript", // actionscript* (starts-with)
                    "dbcp", // *dbcp (ends-with)
                    "jaxb", // *jaxb* (both)
                    "*js",
                    "js*",
                    "*js*",
                    "php",
                    "*php*",
                };
            //var tests = new[] { "*php*" };

            var oldModeTimer = Stopwatch.StartNew();
            var phpMatchesOld = WildcardProcessor.ExpandTagsTrie(allTags, tests.ToList(), trie, useNewMode: false);
            oldModeTimer.Stop();
            Console.WriteLine("\nTesting {{{0}}} OLD mode, took {1:N} ms ({2} expanded tags):\n{3}\n",
                              String.Join(", ", tests), oldModeTimer.ElapsedMilliseconds, phpMatchesOld.Count,
                              String.Join(", ", phpMatchesOld));

            var newModeTimer = Stopwatch.StartNew();
            var phpMatchesNew = WildcardProcessor.ExpandTagsTrie(allTags, tests.ToList(), trie, useNewMode: true);
            newModeTimer.Stop();
            Console.WriteLine("\nTesting {{{0}}} NEW mode, took {1:N} ms ({2} expanded tags):\n{3}\n",
                              String.Join(", ", tests), newModeTimer.ElapsedMilliseconds, phpMatchesNew.Count,
                              String.Join(", ", phpMatchesNew));

            //var tests = new[]
            //    { 
            //        "actionscript", // actionscript* (starts-with)
            //        "dbcp", // *dbcp (ends-with)
            //        "jaxb", // *jaxb* (both)
            //        "js",
            //        //"php",
            //    };
            //foreach (var test in tests)
            //{
            //    Console.WriteLine();
            //    foreach (var item in new[] { test, WildcardProcessor.Reverse(test) })
            //    {
            //        var matches = string.Join(", ", trie.GetByPrefix(item).Select(t =>
            //        {
            //            if (t.Value == 1)
            //                return t.Key; // + " (Forward)";
            //            else if (t.Value == -1)
            //                return WildcardProcessor.Reverse(t.Key) + " (Reversed)";
            //            else if (t.Value == 0)
            //                return t.Key + " (Both)";
            //            else
            //                return t.Key + " (" + t.Value + ")";
            //        }));
            //        Console.WriteLine("{0} -> {1}", item, matches);
            //    }
            //}
            //Console.WriteLine();
        }

        private static void RunExclusionQueryTests(TagServer tagServer, List<string> expandedTags, int runsPerLoop)
        {
            Results.CreateNewFile(string.Format("Results-{0}.csv", DateTime.Now.ToString("yyyy-MM-dd @ HH-mm-ss")));
            //Results.AddHeaders("Count", "Fast", "Fast Alt", "Bloom");
            Results.AddHeaders("Count", "Fast", "Fast Alt");

            var amounts = new List<int>();
            //for (decimal notQueries = (25.0m / 16.0m); notQueries <= 1600; notQueries *= 2)
            for (decimal notQueries = (25.0m / 16.0m); notQueries <= 6400; notQueries *= 2)
            //for (decimal notQueries = 100m; notQueries <= 3200; notQueries *= 2)
            //decimal notQueries = 12.0m;
            {
                amounts.Add((int)notQueries);
            }
            amounts.Add(expandedTags.Count);

            Console.WriteLine("\n\nVarying the number of \"Not Queries\"\n");
            //while (true)
            {
                foreach (var count in amounts)
                {
                    var pageSize = 50;
                    var excludedTags = SelectNItemsFromList(expandedTags, count);
                    //Console.WriteLine("Count={0}, excludedTags.Count={1} tags:{2}\n\n", count, excludedTags.Count, string.Join(", ", excludedTags));
                    //continue;

                    //GC.Collect(2, GCCollectionMode.Forced);
                    for (int i = 0; i < runsPerLoop; i++)
                    {
                        Results.AddData(excludedTags.Count.ToString());

                        //var results1 = tagServer.BooleanQueryWithExclusionsSlowVersion(QueryType.Score, ".net", excludedTags, pageSize: pageSize);
                        //var results2 = tagServer.BooleanQueryWithExclusionsFastVersion(QueryType.Score, ".net", excludedTags, pageSize: pageSize);

                        var results1 = tagServer.BooleanQueryWithExclusionsFastVersion(QueryType.Score, ".net", excludedTags, pageSize: pageSize);
                        var results2 = tagServer.BooleanQueryWithExclusionsFastAlternativeVersion(QueryType.Score, ".net", excludedTags, pageSize: pageSize);
                        //var results3 = tagServer.BooleanQueryWithExclusionsBloomFilterVersion(QueryType.Score, ".net", excludedTags, pageSize: pageSize);
                        CompareLists(listA: results1, nameA: "Fast", listB: results2, nameB: "FastAlternative");
                        //CompareLists(listA: results2, nameA: "FastAlternative", listB: results3, nameB: "Bloom");

                        Results.StartNewRow();
                    }
                    //GC.Collect(2, GCCollectionMode.Forced);
                }
            }

            Results.CloseFile();
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

        private static void RunComparisonQueries(TagServer tagServer)
        {
            var smallTag = tagServer.AllTags.Where(t => t.Value <= 200).First();
            string tag1 = ".net", tag2 = smallTag.Key;
            int pageSize = 10;
            Console.WriteLine("Comparison queries:\n\t\"{0}\" has {1} questions\n\t\"{2}\" has {3} questions\n",
                              tag1, tagServer.AllTags[tag1], tag2, tagServer.AllTags[tag2]);

            Console.WriteLine("\nAND Comparison queries\n");
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "AND", pageSize: pageSize);
            tagServer.ComparisonQuery(QueryType.Score, tag2, tag1, "AND", pageSize: pageSize);
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "AND", pageSize: pageSize);
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "AND", pageSize: pageSize, skip: 100);
            tagServer.ComparisonQuery(QueryType.Score, tag2, tag1, "AND", pageSize: pageSize, skip: 100);
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "AND", pageSize: pageSize, skip: 100);

            Console.WriteLine("\nOR Comparison queries\n");
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "OR", pageSize: pageSize);
            tagServer.ComparisonQuery(QueryType.Score, tag2, tag1, "OR", pageSize: pageSize);
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "OR", pageSize: pageSize);
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "OR", pageSize: pageSize, skip: 100);
            tagServer.ComparisonQuery(QueryType.Score, tag2, tag1, "OR", pageSize: pageSize, skip: 100);
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "OR", pageSize: pageSize, skip: 100);

            Console.WriteLine("\nNOT Comparison queries\n");
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "NOT", pageSize: pageSize);
            tagServer.ComparisonQuery(QueryType.Score, tag2, tag1, "NOT", pageSize: pageSize);
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "NOT", pageSize: pageSize);
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "NOT", pageSize: pageSize, skip: 100);
            tagServer.ComparisonQuery(QueryType.Score, tag2, tag1, "NOT", pageSize: pageSize, skip: 100);
            tagServer.ComparisonQuery(QueryType.Score, tag1, tag2, "NOT", pageSize: pageSize, skip: 100);
        }

        private static TagServer CreateTagServer(List<Question> rawQuestions, bool createFromScratch, string intermediateFilesFolder)
        {
            TagServer tagServer;
            var allTagsFilename = "intermediate-AllTags.bin";
            var allTagsFilePath = Path.Combine(intermediateFilesFolder, allTagsFilename);
            if (createFromScratch)
            {
                tagServer = CreateFromScratch(rawQuestions);
                var serializeTimer = Stopwatch.StartNew();

                Stopwatch itemTimer;
                Console.WriteLine("Serialisation folder: {0}", intermediateFilesFolder);
                foreach (QueryType type in (QueryType[])Enum.GetValues(typeof(QueryType)))
                {
                    var rttFilename = Path.Combine(intermediateFilesFolder, "intermediate-" + type + ".bin");
                    itemTimer = Stopwatch.StartNew();
                    if (File.Exists(rttFilename))
                        File.Delete(rttFilename);
                    using (var file = File.OpenWrite(rttFilename))
                    {
                        Serializer.Serialize(file, tagServer.GetQueryTypeInfo(type));
                    }
                    itemTimer.Stop();
                    Console.WriteLine("Took {0} to serialise to: {1}", itemTimer.Elapsed, "intermediate-" + type + ".bin");
                }

                itemTimer = Stopwatch.StartNew();
                if (File.Exists(allTagsFilePath))
                    File.Delete(allTagsFilePath);
                using (var file = File.OpenWrite(allTagsFilePath))
                {
                    Serializer.Serialize(file, tagServer.AllTags);
                }
                itemTimer.Stop();
                Console.WriteLine("Took {0} to serialise to: {1}", itemTimer.Elapsed, allTagsFilename);

                serializeTimer.Stop();
                Console.WriteLine("\nTook {0} (in TOTAL) to serialise the intermediate data TO disk\n", serializeTimer.Elapsed);
            }
            else
            {
                var queryTypes = (QueryType[])Enum.GetValues(typeof(QueryType));
                var intermediateResults = new Dictionary<QueryType, Dictionary<string, int[]>>(queryTypes.Length);
                var allTags = new TagLookup();
                var deserializeTimer = Stopwatch.StartNew();

                Stopwatch itemTimer;
                Console.WriteLine("Deserialisation folder: {0}", intermediateFilesFolder);
                foreach (QueryType type in queryTypes)
                {
                    var rttFilename = Path.Combine(intermediateFilesFolder, "intermediate-" + type + ".bin");
                    itemTimer = Stopwatch.StartNew();
                    using (var file = File.OpenRead(rttFilename))
                    {
                        var rttTest = Serializer.Deserialize<Dictionary<string, int[]>>(file);
                        intermediateResults.Add(type, rttTest);
                    }
                    itemTimer.Stop();
                    Console.WriteLine("Took {0} to deserialise from: {1}", itemTimer.Elapsed, "intermediate-" + type + ".bin");
                }

                itemTimer = Stopwatch.StartNew();
                using (var file = File.OpenRead(allTagsFilePath))
                {
                    allTags = Serializer.Deserialize<TagLookup>(file);
                }
                itemTimer.Stop();
                Console.WriteLine("Took {0} to deserialise from: {1}", itemTimer.Elapsed, allTagsFilename);

                deserializeTimer.Stop();
                Console.WriteLine("\nTook {0} (in TOTAL) to DE-serialise the intermediate data FROM disk\n", deserializeTimer.Elapsed);

                tagServer = new TagServer(rawQuestions, allTags, intermediateResults);
            }
            return tagServer;
        }

        private static TagServer CreateFromScratch(List<Question> rawQuestions)
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

            var tagServer = new TagServer(rawQuestions);
            //For DEBUGGING only, so we can test on a smaller sample
            //var tagServer = new TagServer(rawQuestions.Take(10 * 1000).ToList()); 

            return tagServer;
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
    }
// ReSharper restore LocalizableElement
}
