using Ewah;
using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using TagByQueryBitMapLookup = System.Collections.Generic.Dictionary<string, Ewah.EwahCompressedBitArray>;
using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;
using TagLookup = System.Collections.Generic.Dictionary<string, int>;

namespace StackOverflowTagServer
{
    internal class BitMapIndexHandler
    {
        private readonly List<Question> questions;
        private readonly TagLookup allTags;
        private readonly Func<QueryType, TagByQueryLookup> GetTagByQueryLookup;
        private readonly Func<QueryType, TagByQueryBitMapLookup> GetTagByQueryBitMapLookup;

        internal BitMapIndexHandler(List<Question> questions,
                                    TagLookup allTags,
                                    Func<QueryType, TagByQueryLookup> getTagByQueryLookup,
                                    Func<QueryType, TagByQueryBitMapLookup> getTagByQueryBitMapLookup)
        {
            this.questions = questions;
            this.allTags = allTags;
            this.GetTagByQueryLookup = getTagByQueryLookup;
            this.GetTagByQueryBitMapLookup = getTagByQueryBitMapLookup;
        }

        internal EwahCompressedBitArray CreateBitMapIndexForExcludedTags(CLR.HashSet<string> tagsToExclude, QueryType queryType)
        {
            var bitMapTimer = Stopwatch.StartNew();
            var tagLookupForQueryType = GetTagByQueryLookup(queryType);
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

            var allQuestions = tagLookupForQueryType[TagServer.ALL_TAGS_KEY];
            var reverseMode = excludedQuestionIds.Count < (questions.Count / 2);
            //var allQuestionLookup = allQuestions.Select((quId, index) => new { quId, index }).ToDictionary(i => i.quId, i => i.index);
            //var reverseMode = true;

            var setBitsTimer = Stopwatch.StartNew();
            for (int index = 0; index < allQuestions.Length; index++)
            {
                bool wasSet = true;
                var shouldExclude = excludedQuestionIds.Contains(allQuestions[index]);
                if (reverseMode && shouldExclude)
                    wasSet = bitMap.Set(index); // Set a bit where you CAN'T use a question, but at the end NOT the whole BitMap (see below)
                else if (reverseMode == false && shouldExclude == false)
                    wasSet = bitMap.Set(index); // Directly set a bit where you CAN use the question (won't do a NOT afterwards)

                if (wasSet == false)
                    Logger.LogStartupMessage("Error, unable to set bit {0:N0} (SizeInBits = {1:N0})", index, bitMap.SizeInBits);
            }

            //for (int index = 0; index < allQuestions.Length; index++)
            //{
            //    var qu = questions[index];
            //    for (int t = 0; t < qu.Tags.Count; t++)
            //    {
            //        var tag = qu.Tags[t];
            //        if (tagsToExclude.Contains(tag))
            //        {
            //            bitMap.Set(index); // Set a bit where you CAN'T use a question, but at the end NOT the whole BitMap (see below)
            //            break; // Stop iterating through tags, we know we can't use this question
            //        }
            //    }
            //}
            //bitMap.Not(); // in-place

            // This is much slower 1,169 ms v. 340 ms (probably the sorting and creating the lookup!!!)
            //var allQuestionLookup = allQuestions.Select((quId, index) => new { quId, index }).ToDictionary(i => i.quId, i => i.index);
            //foreach (var item in excludedQuestionIds.Select(quId => new { quId, index = allQuestionLookup[quId] }).OrderBy(i => i.index))
            //{
            //    bool wasSet = bitMap.Set(item.index); // Set a bit where you CAN'T use a question, but at the end NOT the whole BitMap (see below)

            //    if (wasSet == false)
            //        Logger.LogStartupMessage("Error, unable to set bit {0:N0} (SizeInBits = {1:N0})", item.index, bitMap.SizeInBits);
            //}
            setBitsTimer.Stop();

            var alternativeBitSetTimer = Stopwatch.StartNew();
            var bitArrayLength = CLR.BitHelper.ToIntArrayLength(questions.Count);
            var bitHelperArray = new int[bitArrayLength];
            var bitSet = new CLR.BitHelper(bitHelperArray, bitArrayLength);
            bitSet.SetMaxAllowedBit(questions.Count);

            for (int index = 0; index < allQuestions.Length; index++)
            {
                var shouldExclude = excludedQuestionIds.Contains(allQuestions[index]);
                if (reverseMode && shouldExclude)
                    bitSet.MarkBit(index); // Set a bit where you CAN'T use a question, but at the end NOT the whole BitMap (see below)
                else if (reverseMode == false && shouldExclude == false)
                    bitSet.MarkBit(index); // Directly set a bit where you CAN use the question (won't do a NOT afterwards)
            }

            //for (int index = 0; index < allQuestions.Length; index++)
            //{
            //    var qu = questions[index];
            //    for (int t = 0; t < qu.Tags.Count; t++)
            //    {
            //        var tag = qu.Tags[t];
            //        if (tagsToExclude.Contains(tag))
            //        {
            //            bitSet.MarkBit(index); // Set a bit where you CAN'T use a question, but at the end NOT the whole BitMap (see below)
            //            break; // Stop iterating through tags, we know we can't use this question
            //        }
            //    }
            //}
            //bitSet.Not(); // in-place

            // This is also MUCH sloower, 2,707 ms v. 378 ms (mostly down to the time taken to make the lookup!!)
            //var allQuestionLookup = allQuestions.Select((quId, index) => new { quId, index }).ToDictionary(i => i.quId, i => i.index);
            //foreach (var questionId in excludedQuestionIds)
            //{
            //    var index = allQuestionLookup[questionId];
            //    bitSet.MarkBit(index); // Set a bit where you CAN'T use a question, but at the end NOT the whole BitMap (see below)
            //}
            alternativeBitSetTimer.Stop();

            //var bitMapSizeBefore = (long)bitMap.GetCardinality();
            //var bitSetSizeBefore = (long)bitSet.GetCardinality();
            //var diffBefore = bitMapSizeBefore - bitSetSizeBefore;
            //if (bitMapSizeBefore != bitSetSizeBefore) // && bitMap.GetCardinality() < 7000000)
            //{
            //    var bitMapPositions = bitMap.GetPositions();
            //    var bitSetPositions = bitSet.GetPositions();

            //    var inFirstButNotInSecond = new HashSet<int>(bitMapPositions).Except(bitSetPositions).ToList();
            //    var inSecondButNotInFirst = new HashSet<int>(bitSetPositions).Except(bitMapPositions).ToList();
            //    var tempHashSet = new HashSet<int>(bitMapPositions);
            //    tempHashSet.IntersectWith(bitSetPositions);
            //    var inBoth = tempHashSet.ToList();

            //    var temp = 5;
            //}

            var tidyUpTimer = Stopwatch.StartNew();
            bitMap.SetSizeInBits(questions.Count, defaultvalue: false);
            bitMap.Shrink();
            tidyUpTimer.Stop();

            var notTimer = Stopwatch.StartNew();
            if (reverseMode)
            {
                bitMap.Not(); // in-place
                bitSet.Not(); // in-place
            }
            notTimer.Stop();

            bitMapTimer.Stop();

            //var bitMapSizeAfter = (long)bitMap.GetCardinality();
            //var bitSetSizeAfter = (long)bitSet.GetCardinality();
            //var diffAfter = bitMapSizeAfter - bitSetSizeAfter;
            //if (bitMapSizeAfter != bitSetSizeAfter)
            //{
            //    var bitMapPositions = bitMap.GetPositions();
            //    var bitSetPositions = bitSet.GetPositions();

            //    var inFirstButNotInSecond = new HashSet<int>(bitMapPositions).Except(bitSetPositions).ToList();
            //    var inSecondButNotInFirst = new HashSet<int>(bitSetPositions).Except(bitMapPositions).ToList();
            //    var tempHashSet = new HashSet<int>(bitMapPositions);
            //    tempHashSet.IntersectWith(bitSetPositions);
            //    var inBoth = tempHashSet.ToList();

            //    var temp = 5;
            //}

            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to collect {2:N0} Question Ids from {3:N0} Tags",
                                     collectIdsTimer.Elapsed, collectIdsTimer.ElapsedMilliseconds, excludedQuestionIds.Count, tagsToExclude.Count);
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to set {2:N0} bits {3}",
                                     setBitsTimer.Elapsed, setBitsTimer.ElapsedMilliseconds,
                                     reverseMode ? ((ulong)questions.Count - bitMap.GetCardinality()) : bitMap.GetCardinality(),
                                     reverseMode ? "(in REVERSE mode)" : "");
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to set {2:N0} bits using ALTERNATIVE mode {3}",
                                     alternativeBitSetTimer.Elapsed, alternativeBitSetTimer.ElapsedMilliseconds,
                                     (reverseMode ? (questions.Count - bitSet.GetCardinality()) : bitSet.GetCardinality()),
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

        internal void CreateBitMapIndexes()
        {
            // First create all the BitMap Indexes we'll need, one per/Tag, per/QueryType
            var bitSetsTimer = Stopwatch.StartNew();
            var tagsToUse = GetTagsToUseForBitMapIndexes(minQuestionsPerTag: 0);
            //var tagsToUse = GetTagsToUseForBitMapIndexes(minQuestionsPerTag: 500); // 3,975 Tags with MORE than 500 questions
            //var tagsToUse = GetTagsToUseForBitMapIndexes(minQuestionsPerTag: 1000); // 2,397 Tags with MORE than 1,000 questions
            //var tagsToUse = GetTagsToUseForBitMapIndexes(minQuestionsPerTag: 50000); // 48 Tags with MORE than 50,000 questions
            foreach (var tagToUse in tagsToUse)
            {
                foreach (QueryType type in (QueryType[])Enum.GetValues(typeof(QueryType)))
                {
                    GetTagByQueryBitMapLookup(type).Add(tagToUse, new EwahCompressedBitArray());
                }
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
                var questionsForQuery = GetTagByQueryLookup(queryType)[TagServer.ALL_TAGS_KEY];
                var sanityCheck = new Dictionary<string, int>();
                var bitSetsForQuery = GetTagByQueryBitMapLookup(queryType);
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
                foreach (QueryType type in (QueryType[])Enum.GetValues(typeof(QueryType)))
                {
                    var bitMapIndex = GetTagByQueryBitMapLookup(type);
                    bitMapIndex[tagToUse].SetSizeInBits(questions.Count, defaultvalue: false);
                    bitMapIndex[tagToUse].Shrink();
                }
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
    }
}
