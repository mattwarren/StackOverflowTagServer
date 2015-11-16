using Ewah;
using Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Threading;
using StackOverflowTagServer.Querying;
using StackOverflowTagServer.DataStructures;

using TagByQueryBitMapLookup = System.Collections.Generic.Dictionary<string, Ewah.EwahCompressedBitArray>;
using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;
using TagLookup = System.Collections.Generic.Dictionary<string, int>;

namespace StackOverflowTagServer
{
    internal class Validator
    {
        private readonly List<Question> questions;
        private readonly TagLookup allTags;
        private readonly Func<QueryType, TagByQueryLookup> GetTagByQueryLookup;
        private readonly Func<QueryType, TagByQueryBitMapLookup> GetTagByQueryBitMapLookup;

        internal Validator(List<Question> questions,
                                    TagLookup allTags,
                                    Func<QueryType, TagByQueryLookup> getTagByQueryLookup,
                                    Func<QueryType, TagByQueryBitMapLookup> getTagByQueryBitMapLookup)
        {
            this.questions = questions;
            this.allTags = allTags;
            this.GetTagByQueryLookup = getTagByQueryLookup;
            this.GetTagByQueryBitMapLookup = getTagByQueryBitMapLookup;
        }

        internal void ValidateTagOrdering()
        {
            var validationTimer = Stopwatch.StartNew();

            ValidateItems(GetTagByQueryLookup(QueryType.LastActivityDate).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => qu.LastActivityDate <= prev.LastActivityDate,
                                    "Tags-" + QueryType.LastActivityDate);

            ValidateItems(GetTagByQueryLookup(QueryType.CreationDate).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => qu.CreationDate <= prev.CreationDate,
                                    "Tags-" + QueryType.CreationDate);

            ValidateItems(GetTagByQueryLookup(QueryType.Score).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => Nullable.Compare(qu.Score, prev.Score) <= 0,
                                    "Tags-" + QueryType.Score);

            ValidateItems(GetTagByQueryLookup(QueryType.ViewCount).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => Nullable.Compare(qu.ViewCount, prev.ViewCount) <= 0,
                                    "Tags-" + QueryType.ViewCount);

            ValidateItems(GetTagByQueryLookup(QueryType.AnswerCount).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                                    (qu, prev) => Nullable.Compare(qu.AnswerCount, prev.AnswerCount) <= 0,
                                    "Tags-" + QueryType.AnswerCount);

            validationTimer.Stop();
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to VALIDATE the {2:N0} arrays\n",
                                     validationTimer.Elapsed, validationTimer.ElapsedMilliseconds, allTags.Count * 5);
        }

        internal void ValidateBitMapIndexOrdering()
        {
            var validationTimer = Stopwatch.StartNew();
            ValidateItems(GetTagByQueryBitMapLookup(QueryType.LastActivityDate).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                            (qu, prev) => qu.LastActivityDate <= prev.LastActivityDate,
                            "BitMaps-" + QueryType.LastActivityDate,
                            questionLookup: GetTagByQueryLookup(QueryType.LastActivityDate)[TagServer.ALL_TAGS_KEY]);

            ValidateItems(GetTagByQueryBitMapLookup(QueryType.CreationDate).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                            (qu, prev) => qu.CreationDate <= prev.CreationDate,
                            "BitMaps-" + QueryType.CreationDate,
                            questionLookup: GetTagByQueryLookup(QueryType.CreationDate)[TagServer.ALL_TAGS_KEY]);

            ValidateItems(GetTagByQueryBitMapLookup(QueryType.Score).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                            (qu, prev) => Nullable.Compare(qu.Score, prev.Score) <= 0,
                            "BitMaps-" + QueryType.Score,
                            questionLookup: GetTagByQueryLookup(QueryType.Score)[TagServer.ALL_TAGS_KEY]);

            ValidateItems(GetTagByQueryBitMapLookup(QueryType.ViewCount).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                            (qu, prev) => Nullable.Compare(qu.ViewCount, prev.ViewCount) <= 0,
                            "BitMaps-" + QueryType.ViewCount,
                            questionLookup: GetTagByQueryLookup(QueryType.ViewCount)[TagServer.ALL_TAGS_KEY]);

            ValidateItems(GetTagByQueryBitMapLookup(QueryType.AnswerCount).ToDictionary(item => item.Key, item => item.Value as IEnumerable<int>),
                            (qu, prev) => Nullable.Compare(qu.AnswerCount, prev.AnswerCount) <= 0,
                            "BitMaps-" + QueryType.AnswerCount,
                            questionLookup: GetTagByQueryLookup(QueryType.AnswerCount)[TagServer.ALL_TAGS_KEY]);

            validationTimer.Stop();
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to VALIDATE all the {2:N0} Bit Map Indexes\n",
                                     validationTimer.Elapsed, validationTimer.ElapsedMilliseconds, allTags.Count * 5);
        }

        private void ValidateItems(Dictionary<string, IEnumerable<int>> itemsToCheck,
                                    Func<Question, Question, bool> checker,
                                    string info,
                                    int[] questionLookup = null)
        {
            var timer = Stopwatch.StartNew();
            var globalCounter = 0;
            foreach (var item in itemsToCheck)
            {
                Question previous = null;
                var counter = 0;
                var tag = item.Key;
                foreach (var id in item.Value)
                {
                    Question current = questionLookup == null ? questions[id] : questions[questionLookup[id]];
                    if (previous != null)
                    {
                        var result = checker(current, previous);

                        if (!result)
                        {
                            Logger.LogStartupMessage("Failed with Id {0}, Tag {1}, checker() returned false", id, tag);
                            break;
                        }

                        if (tag != TagServer.ALL_TAGS_KEY && current.Tags.Any(t => t == tag) == false)
                        {
                            Logger.LogStartupMessage("Failed with Id {0}, Expected Tag {1}, Got Tags {2}", id, tag, string.Join(", ", current.Tags));
                            break;
                        }
                    }

                    previous = current;
                    counter++;
                }

                globalCounter += counter;
                if (counter != item.Value.Count())
                    Logger.LogStartupMessage("ERROR - Tag {0}, Checked {1} items, Expected to Check {2} items", tag, counter, item.Value.Count());
            }
            timer.Stop();

            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to SUCCESSFULLY validate {2:N0} items -> {3}",
                                     timer.Elapsed, timer.ElapsedMilliseconds, globalCounter, info);
        }

        internal List<Question> GetInvalidResults(List<Question> results, QueryInfo queryInfo)
        {
            var invalidResults = new List<Question>();
            switch (queryInfo.Operator)
            {
                case "AND":
                    var andMatches = results.Where(q => q.Tags.Any(t => t == queryInfo.Tag) && q.Tags.Any(t => t == queryInfo.OtherTag));
                    invalidResults.AddRange(results.Except(andMatches));
                    break;
                case "AND-NOT":
                    var andNotMatches = results.Where(q => q.Tags.Any(t => t == queryInfo.Tag) && q.Tags.All(t => t != queryInfo.OtherTag));
                    invalidResults.AddRange(results.Except(andNotMatches));
                    break;

                case "OR":
                    var orMatches = results.Where(q => q.Tags.Any(t => t == queryInfo.Tag) || q.Tags.Any(t => t == queryInfo.OtherTag));
                    invalidResults.AddRange(results.Except(orMatches));
                    break;
                case "OR-NOT":
                    var orNotMatches = results.Where(q => q.Tags.Any(t => t == queryInfo.Tag) || q.Tags.All(t => t != queryInfo.OtherTag));
                    invalidResults.AddRange(results.Except(orNotMatches));
                    break;

                // TODO Work out what a "NOT" query really means, at the moment it's the same as "AND-NOT"?!
                //case "NOT":
                //    var notMatches = results.Where(q => q.Tags.Any(t => t == queryInfo.Tag && t != queryInfo.OtherTag));
                //    invalidResults.AddRange(results.Except(notMatches));
                //    break;

                default:
                    throw new InvalidOperationException(string.Format("Invalid operator specified: {0}", queryInfo.Operator ?? "<NULL>"));
            }
            return invalidResults;
        }

        internal List<Tuple<Question, List<string>>> GetShouldHaveBeenExcludedResults(List<Question> results, QueryInfo queryInfo, CLR.HashSet<string> tagsToExclude)
        {
            var errors = new List<Tuple<Question, List<string>>>();
            if (tagsToExclude == null)
                return errors;

            foreach (var result in results)
            {
                var invalidTags = new List<string>();
                foreach (var tag in result.Tags)
                {
                    if (tagsToExclude.Contains(tag))
                        invalidTags.Add(tag);
                }
                if (invalidTags.Count > 0)
                    errors.Add(Tuple.Create(result, invalidTags));
            }
            return errors;
        }

        internal void ValidateExclusionBitMap(EwahCompressedBitArray bitMapIndex, CLR.HashSet<string> expandedTagsNGrams, QueryType queryType)
        {
            // Exclusion BitMap is Set (i.e. 1) in places where you CAN use the question, i.e. it's NOT excluded
            var questionLookup = GetTagByQueryLookup(queryType)[TagServer.ALL_TAGS_KEY];
            var invalidQuestions = new List<Tuple<Question, string>>();
            var NOTbitMapIndex = ((EwahCompressedBitArray)bitMapIndex.Clone());
            NOTbitMapIndex.Not();
            var positions = NOTbitMapIndex.GetPositions();
            foreach (var position in positions)
            {
                var question = questions[questionLookup[position]];
                foreach (var tag in question.Tags)
                {
                    if (expandedTagsNGrams.Contains(tag))
                        invalidQuestions.Add(Tuple.Create(question, tag));
                }
                // Sometimes the validitation locks up my laptop, this *seems* to make a difference?!
                Thread.Yield();
            }

            using (Utils.SetConsoleColour(ConsoleColor.Blue))
                Logger.Log("Validating Exclusion Bit Map, checked {0:N0} positions for INVALID tags", positions.Count);

            if (invalidQuestions.Any())
            {
                using (Utils.SetConsoleColour(ConsoleColor.Red))
                    Logger.Log("ERROR Validating Exclusion Bit Map, {0:N0} questions should have been excluded",
                               invalidQuestions.Select(i => i.Item1.Id).Distinct().Count());

                foreach (var error in invalidQuestions)
                {
                    Logger.Log("  {0,8}: {1} -> {2}", error.Item1.Id, String.Join(", ", error.Item1.Tags), error.Item2);
                }
            }


            var expectedPositions = bitMapIndex.GetPositions();
            foreach (var position in expectedPositions)
            {
                var question = questions[questionLookup[position]];
                if (question.Tags.Any(t => expandedTagsNGrams.Contains(t)) == false)
                {
                    using (Utils.SetConsoleColour(ConsoleColor.Red))
                        Logger.Log("ERROR {0,8}: {1} -> didn't contain ANY excluded tags", question.Id, String.Join(", ", question.Tags));
                }
            }

            using (Utils.SetConsoleColour(ConsoleColor.Blue))
                Logger.Log("Validating Exclusion Bit Map, checked {0:N0} positions for EXPECTED tags", expectedPositions.Count);

            Logger.Log();
        }
    }
}
