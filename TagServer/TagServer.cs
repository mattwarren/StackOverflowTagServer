using ProtoBuf;
using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace StackOverflowTagServer
{
    public class TagServer
    {
        public delegate void LogAction(string format, params object[] args);

        private readonly Dictionary<string, int> allTags;
        public Dictionary<string, int> AllTags { get { return allTags; } }

        private readonly List<Question> questions;
        public List<Question> Questions { get { return questions; } }

        private readonly List<string> messages = new List<string>();
        public List<string> Messages { get { return messages; } }

        private readonly Dictionary<string, List<KeyValuePair<string, int>>> relatedTags;

        // GetQueryTypeInfo(QueryType type) Maps these Dictionaries to a QueryType (enum)
        private readonly Dictionary<string, int[]> tagsByAnswerCount;
        private readonly Dictionary<string, int[]> tagsByCreationDate;
        private readonly Dictionary<string, int[]> tagsByLastActivityDate;
        private readonly Dictionary<string, int[]> tagsByScore;
        private readonly Dictionary<string, int[]> tagsByViewCount;

        private readonly QueryProcessor queryProcessor;

        public TagServer(List<Question> questionsList)
        {
            questions = questionsList;
            queryProcessor = new QueryProcessor(questions, type => GetQueryTypeInfo(type));

            var groupedTags = CreateTagGroupings();
            relatedTags = CreateRelatedTags(groupedTags);

            allTags = groupedTags.ToDictionary(t => t.Key, t => t.Value.Count);

            tagsByLastActivityDate = new Dictionary<string, int[]>(groupedTags.Count);
            tagsByCreationDate = new Dictionary<string, int[]>(groupedTags.Count);
            tagsByScore = new Dictionary<string, int[]>(groupedTags.Count);
            tagsByViewCount = new Dictionary<string, int[]>(groupedTags.Count);
            tagsByAnswerCount = new Dictionary<string, int[]>(groupedTags.Count);

            CreateSortedLists(groupedTags);

            GC.Collect(2, GCCollectionMode.Forced);
            Log("After SETUP - Using {0:0.00} MB of memory in total\n", GC.GetTotalMemory(true) / 1024.0 / 1024.0);

            ValidateTagOrdering();
        }

        internal TagServer(List<Question> questionsList, Dictionary<string, int> allTags, 
                           Dictionary<QueryType, Dictionary<string, int[]>> intermediateValues)
        {
            questions = questionsList;
            queryProcessor = new QueryProcessor(questions, type => GetQueryTypeInfo(type));
            this.allTags = allTags;

            tagsByAnswerCount = intermediateValues[QueryType.AnswerCount];
            tagsByCreationDate = intermediateValues[QueryType.CreationDate];
            tagsByLastActivityDate = intermediateValues[QueryType.LastActivityDate];
            tagsByScore = intermediateValues[QueryType.Score];
            tagsByViewCount = intermediateValues[QueryType.ViewCount];

            GC.Collect(2, GCCollectionMode.Forced);
            Log("After SETUP - Using {0:0.00} MB of memory in total\n", GC.GetTotalMemory(true) / 1024.0 / 1024.0);

            // This takes a while, maybe don't do it when using Intermediate results (that have already has this check?)
            //ValidateTagOrdering();
        }

        public static TagServer CreateFromFile(string filename)
        {
            List<Question> rawQuestions;
            var memoryBefore = GC.GetTotalMemory(true);
            var fileReadTimer = Stopwatch.StartNew();
            using (var file = File.OpenRead(filename))
            {
                rawQuestions = Serializer.Deserialize<List<Question>>(file);
            }
            fileReadTimer.Stop();
            var memoryAfter = GC.GetTotalMemory(true);

            var tagServer = new TagServer(rawQuestions);
            tagServer.Log("Took {0} ({1:N0} ms) to DE-serialise {2:N0} Stack Overflow Questions from the file, used {3:0.00} MB of memory\n",
                                fileReadTimer.Elapsed, fileReadTimer.Elapsed.TotalMilliseconds, rawQuestions.Count, (memoryAfter - memoryBefore) / 1024.0 / 1024.0);
            return tagServer;
        }

        public int TotalCount(QueryType type, string tag)
        {
            Dictionary<string, int[]> queryInfo = GetQueryTypeInfo(type);
            return queryInfo[tag].Length;
        }

        public List<Question> Query(QueryType type, string tag, int pageSize = 50, int skip = 0 /*, bool ascending = true*/)
        {
            return queryProcessor.Query(type, tag, pageSize, skip);
        }

        public List<Question> ComparisonQuery(QueryType type, string tag1, string tag2, string @operator, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.ComparisonQuery(type, tag1, tag2, @operator, pageSize, skip);
        }

        public List<Question> BooleanQueryWithExclusionsSlowVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize = 50, int skip = 0)
        {
            return queryProcessor.BooleanQueryWithExclusionsSlowVersion(type, tag, excludedTags, pageSize, skip);
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

        public Dictionary<string, int[]> GetQueryTypeInfo(QueryType type)
        {
            Dictionary<string, int[]> queryInfo;
            switch (type)
            {
                case QueryType.LastActivityDate:
                    queryInfo = tagsByLastActivityDate;
                    break;
                case QueryType.CreationDate:
                    queryInfo = tagsByCreationDate;
                    break;
                case QueryType.Score:
                    queryInfo = tagsByScore;
                    break;
                case QueryType.ViewCount:
                    queryInfo = tagsByViewCount;
                    break;
                case QueryType.AnswerCount:
                    queryInfo = tagsByAnswerCount;
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Invalid query type {0}", (int)type));
            }
            return queryInfo;
        }

        private Dictionary<string, List<KeyValuePair<string, int>>> CreateRelatedTags(Dictionary<string, TagWithPositions> groupedTags)
        {
            var memoryBefore = GC.GetTotalMemory(true);
            var relatedTagsTimer = Stopwatch.StartNew();
            var relatedTagsTemp = new Dictionary<string, Dictionary<string, int>>(groupedTags.Count);
            foreach (var tag in groupedTags)
            {
                relatedTagsTemp.Add(tag.Key, new Dictionary<string, int>());
            }
            foreach (var question in questions)
            {
                foreach (var tag in question.Tags)
                {
                    foreach (var otherTag in question.Tags)
                    {
                        if (tag == otherTag)
                            continue;

                        if (relatedTagsTemp[tag].ContainsKey(otherTag))
                            relatedTagsTemp[tag][otherTag]++;
                        else
                            relatedTagsTemp[tag].Add(otherTag, 1);
                    }
                }
            }
            var relatedTags = new Dictionary<string, List<KeyValuePair<string, int>>>(groupedTags.Count);
            foreach (var tag in relatedTagsTemp)
            {
                // Now we can go back and sort the related tags, so they are in descending order
                relatedTags.Add(tag.Key, tag.Value.OrderByDescending(i => i.Value).ToList());
            }
            relatedTagsTimer.Stop();
            var memoryAfter = GC.GetTotalMemory(true);
            Log("Took {0} ({1:N0} ms) to create all the \"related\" tags info, used {2:0.00} MB of memory\n",
                        relatedTagsTimer.Elapsed, relatedTagsTimer.Elapsed.TotalMilliseconds, (memoryAfter - memoryBefore) / 1024.0 / 1024.0);
            return relatedTags;
        }

        private void CreateSortedLists(Dictionary<string, TagWithPositions> groupedTags)
        {
            var comparer = new Comparer(questions);
            var sortingTimer = Stopwatch.StartNew();
            foreach (var tag in groupedTags)
            {
                var byLastActivityDate = tag.Value.Positions.ToArray(); // HAVE to call this each time, we want a copy!!
                Array.Sort(byLastActivityDate, comparer.LastActivityDate);
                tagsByLastActivityDate.Add(tag.Key, byLastActivityDate);

                var byCreationDate = tag.Value.Positions.ToArray(); // HAVE to call this each time, we want a copy!!
                Array.Sort(byCreationDate, comparer.CreationDate);
                tagsByCreationDate.Add(tag.Key, byCreationDate);

                var byScore = tag.Value.Positions.ToArray(); // HAVE to call this each time, we want a copy!!
                Array.Sort(byScore, comparer.Score);
                tagsByScore.Add(tag.Key, byScore);

                var byViewCount = tag.Value.Positions.ToArray(); // HAVE to call this each time, we want a copy!!
                Array.Sort(byViewCount, comparer.ViewCount);
                tagsByViewCount.Add(tag.Key, byViewCount);

                var byAnswerCount = tag.Value.Positions.ToArray(); // HAVE to call this each time, we want a copy!!
                Array.Sort(byAnswerCount, comparer.AnswerCount);
                tagsByAnswerCount.Add(tag.Key, byAnswerCount);
            }
            sortingTimer.Stop();
            Log("Took {0} ({1:N0} ms) to sort the {2:N0} arrays\n",
                            sortingTimer.Elapsed, sortingTimer.ElapsedMilliseconds, groupedTags.Count * 5);
        }

        private Dictionary<string, TagWithPositions> CreateTagGroupings()
        {
            var memoryBefore = GC.GetTotalMemory(true);
            var tagGroupingTimer = Stopwatch.StartNew();
            // Could **possibly** optimise this by doing it without LINQ?!?
            // Or maybe just use LINQOptimiser to do it for us?!
            var tagsWithQuestionsIds = questions.SelectMany((qu, n) => qu.Tags.Select(t => new
                                                                {
                                                                    Tag = t,
                                                                    Position = n
                                                                }),
                                                            (qu, tag) => tag)
                                        .ToLookup(x => x.Tag)
                                        .Select(x => new TagServer.TagWithPositions()
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
            tagGroupingTimer.Stop();
            var memoryAfter = GC.GetTotalMemory(true);
            Log("Took {0} ({1:N0} ms) to group all the tags, used {2:0.00} MB of memory\n",
                        tagGroupingTimer.Elapsed, tagGroupingTimer.ElapsedMilliseconds, (memoryAfter - memoryBefore) / 1024.0 / 1024.0);
            return tagsWithQuestionsIds;
        }

        private void ValidateTagOrdering()
        {
            var validator = new Validator(questions, (format, args) => Log(format, args));
            var validationTimer = Stopwatch.StartNew();
            validator.ValidateTags(GetQueryTypeInfo(QueryType.LastActivityDate), (qu, prev) => qu.LastActivityDate <= prev.LastActivityDate);
            validator.ValidateTags(GetQueryTypeInfo(QueryType.CreationDate), (qu, prev) => Nullable.Compare<DateTime>(qu.CreationDate, prev.CreationDate) <= 0);
            validator.ValidateTags(GetQueryTypeInfo(QueryType.Score), (qu, prev) => Nullable.Compare(qu.Score, prev.Score) <= 0);
            validator.ValidateTags(GetQueryTypeInfo(QueryType.ViewCount), (qu, prev) => Nullable.Compare(qu.ViewCount, prev.ViewCount) <= 0);
            validator.ValidateTags(GetQueryTypeInfo(QueryType.AnswerCount), (qu, prev) => Nullable.Compare(qu.AnswerCount, prev.AnswerCount) <= 0);
            validationTimer.Stop();
            Log("Took {0} ({1:N0} ms) to VALIDATE all the {2:N0} arrays\n",
                  validationTimer.Elapsed, validationTimer.ElapsedMilliseconds, allTags.Count * 5);
        }

        private void Log(string format, params object[] args)
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
