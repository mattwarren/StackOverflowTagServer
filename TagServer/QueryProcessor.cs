using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;

using HashSet = StackOverflowTagServer.CLR.HashSet<int>;
//using HashSet = System.Collections.Generic.HashSet<int>;
using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;

namespace StackOverflowTagServer
{
    internal class QueryProcessor
    {
        private readonly List<Question> questions;
        private readonly Func<QueryType, TagByQueryLookup> GetQueryTypeInfo;

        private readonly Lazy<HashSet> HashSetCache = new Lazy<HashSet>(() => 
            {
                var hashSet = new HashSet(new IntComparer());
                var initialiseMethod = typeof(HashSet).GetMethod("Initialize", BindingFlags.NonPublic | BindingFlags.Instance);
                var exclusionCount = 8500000; // 8.5 million, more than enough, our data-set only has 7.9million questions!
                initialiseMethod.Invoke(hashSet, new object[] { exclusionCount });
                return hashSet;
            },
            LazyThreadSafetyMode.ExecutionAndPublication);

        internal QueryProcessor(List<Question> questions, Func<QueryType, TagByQueryLookup> getQueryTypeInfo)
        {
            this.questions = questions;
            this.GetQueryTypeInfo = getQueryTypeInfo;
        }

        internal List<Question> Query(QueryType type, string tag, int pageSize, int skip)
        {
            var timer = Stopwatch.StartNew();
            TagByQueryLookup queryInfo = GetQueryTypeInfo(type);
            Func<Question, string> fieldSelector = GetFieldSelector(type);
            ThrowIfInvalidParameters(tag, pageSize, queryInfo);

            var result = queryInfo[tag]
                // TODO how to efficiently deal with ascending (i.e. start at the end and work backwards)
                .Skip(skip)
                .Take(pageSize)
                .Select(i => questions[i])
                .ToList();
            timer.Stop();

            using (SetConsoleColour(GetColorForTimespan(timer.Elapsed)))
            {
                Console.WriteLine("Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms)",
                                    type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            }
            var formattedResults = result.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            Console.WriteLine("  {0}", string.Join("\n  ", formattedResults));
            Console.WriteLine("\n");

            return result;
        }

        internal List<Question> ComparisonQuery(QueryType type, string tag1, string tag2, string @operator, int pageSize, int skip)
        {
            var timer = Stopwatch.StartNew();
            TagByQueryLookup queryInfo = GetQueryTypeInfo(type);
            Func<Question, string> fieldSelector = GetFieldSelector(type);
            ThrowIfInvalidParameters(tag1, pageSize, queryInfo);
            ThrowIfInvalidParameters(tag2, pageSize, queryInfo);

            var baseQueryCounter = 0;
            IEnumerable<int> baseQuery = queryInfo[tag1]
                .Select(i =>
                {
                    baseQueryCounter++;
                    return i;
                });
            switch (@operator)
            {
                //Use Intersect for AND, Union for OR and Except for NOT
                case "AND":
                    baseQuery = baseQuery.Intersect(queryInfo[tag2]);
                    break;
                case "OR":
                    baseQuery = baseQuery.Union(queryInfo[tag2]);
                    break;
                case "NOT":
                    baseQuery = baseQuery.Except(queryInfo[tag2]);
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Invalid operator specified: {0}", @operator ?? "<NULL>"));
            }

            var result = baseQuery.Skip(skip)
                            .Take(pageSize)
                            .Select(i => questions[i])
                            .ToList();
            timer.Stop();

            Console.WriteLine("Boolean Query: \"{0}\" {1} \"{2}\", pageSize = {3}, skip = {4}, took {5} ({6:N2} ms)",
                               tag1, @operator, tag2, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            Console.WriteLine("Got {0} results in total, baseQueryCounter = {1}", result.Count(), baseQueryCounter);
            var formattedResults = result.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            Console.WriteLine("  {0}", string.Join("\n  ", formattedResults));
            Console.WriteLine("\n");

            return result;
        }

        internal List<Question> BooleanQueryWithExclusionsSlowVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize, int skip)
        {
            var gcInfo = new GCCollectionInfo();
            var timer = Stopwatch.StartNew();

            TagByQueryLookup queryInfo = GetQueryTypeInfo(type);
            Func<Question, string> fieldSelector = GetFieldSelector(type);
            ThrowIfInvalidParameters(tag, pageSize, queryInfo);

            IEnumerable<int> baseQuery = queryInfo[tag];
            foreach (var excludedTag in excludedTags)
            {
                baseQuery = baseQuery.Except(queryInfo[excludedTag]);
            }
            var results = baseQuery.Skip(skip)
                                .Take(pageSize)
                                .Select(i => questions[i])
                                .ToList();
            timer.Stop();
            gcInfo.UpdateCollectionInfo();

            Console.WriteLine("Base Query: {0}, there are {1:N0} Excluded Tags", tag, excludedTags.Count);
            Console.WriteLine("Boolean Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms) - SLOW",
                                    type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            Console.WriteLine("Got {0} results", results.Count());
            Console.WriteLine(gcInfo.ToString());
            //if (pageSize <= 50)
            //{
            //    var formattedResults = results.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //    Console.WriteLine("  {0}", string.Join("\n  ", formattedResults));
            //}
            Console.WriteLine();

            return results;
        }

        /// <summary>
        /// Load up the HashSet with the values from the Base Query, then loop through all the exclusions and remove them
        /// Expensive when their are LOTS of exclusions, expensive when there is a large Base Query (we process it all regardless)
        /// </summary>
        internal List<Question> BooleanQueryWithExclusionsFastVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize, int skip)
        {
            var gcInfo = new GCCollectionInfo();
            var timer = Stopwatch.StartNew();

            TagByQueryLookup queryInfo = GetQueryTypeInfo(type);
            Func<Question, string> fieldSelector = GetFieldSelector(type);
            ThrowIfInvalidParameters(tag, pageSize, queryInfo);

            var baseHashSet = HashSetCache.Value;
            baseHashSet.Clear();
            baseHashSet.UnionWith(queryInfo[tag]);
            foreach (var excludedTag in excludedTags)
            {
                foreach (var qu in queryInfo[excludedTag])
                {
                    // We don't care if it was present before or not, either way it's removed
                    baseHashSet.Remove(qu);
                }
            }
            var results = baseHashSet.Skip(skip)
                                .Take(pageSize)
                                .Select(i => questions[i])
                                .ToList();
            timer.Stop();
            gcInfo.UpdateCollectionInfo();

            Console.WriteLine("Base Query: {0}, there are {1:N0} Excluded Tags", tag, excludedTags.Count);
            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));
            using (SetConsoleColour(GetColorForTimespan(timer.Elapsed)))
            {
                Console.WriteLine("Boolean Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms) - FAST",
                                        type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            }
            Console.WriteLine("Got {0} results, {1:N0} items left in baseHashSet", results.Count(), baseHashSet.Count);
            Console.WriteLine(gcInfo.ToString());
            //var formattedResults = results.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //Console.WriteLine("  {0}", string.Join("\n  ", formattedResults));
            Console.WriteLine();

            return results;
        }

        /// <summary>
        /// Load all the exclusions into a HashSet, then loop through the Base Query, until we have pageSize + Skip items that aren't in the HashSet.
        /// Expensive when there are LOTS of exclusions, but cheaper when the BaseQuery is large because we don't process all of it (stop when we have enough)
        /// </summary>
        internal List<Question> BooleanQueryWithExclusionsFastAlternativeVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize, int skip)
        {
            var gcInfo = new GCCollectionInfo();
            var timer = Stopwatch.StartNew();

            TagByQueryLookup queryInfo = GetQueryTypeInfo(type);
            Func<Question, string> fieldSelector = GetFieldSelector(type);
            ThrowIfInvalidParameters(tag, pageSize, queryInfo);

            //var exclusionCount = 8000000; // 8 million, more than enough (getting an exact count could be expensive)!!
            //var exclusions = new HashSet(new IntComparer());
            ////ensure the HashSet is pre-sized, so it doesn't have to re-size as we add items
            //initialiseMethod.Invoke(exclusions, new object[] { exclusionCount });
            var exclusions = HashSetCache.Value;
            exclusions.Clear();
            foreach (var excludedTag in excludedTags)
            {
                foreach (var qu in queryInfo[excludedTag])
                {
                    exclusions.Add(qu);
                }
            }
            var allResults = new List<int>(skip + pageSize);
            foreach (var qu in queryInfo[tag])
            {
                // If it's not in the exclusions, we can use it
                if (exclusions.Contains(qu) == false)
                    allResults.Add(qu);
                if (allResults.Count >= (skip + pageSize))
                    break;
            }
            var results = allResults.Skip(skip)
                                .Take(pageSize)
                                .Select(i => questions[i])
                                .ToList();
            timer.Stop();
            gcInfo.UpdateCollectionInfo();

            Console.WriteLine("Base Query: {0}, there are {1:N0} Excluded Tags", tag, excludedTags.Count);
            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));
            using (SetConsoleColour(GetColorForTimespan(timer.Elapsed)))
            {
                Console.WriteLine("Boolean Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms) - FAST ALT",
                                        type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            }
            Console.WriteLine("Got {0} results ({1} in allResults), {2:N0} items in exclusions", results.Count(), allResults.Count, exclusions.Count);
            Console.WriteLine(gcInfo.ToString());
            //var formattedResults = results.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //Console.WriteLine("  {0}", string.Join("\n  ", formattedResults));
            Console.WriteLine();

            return results;
        }

        /// <summary>
        /// Similar to <seealso cref="BooleanQueryWithExclusionsFastAlternativeVersion"/> using a BloomFilter instead of a HashSet
        /// Load up the BloomFilter with the exclusions, then loop through the Base Query, until we have pageSize + Skip items that aren't in the BloomFilter.
        /// Expensive when there are LOTS of exclusions, but cheaper when the BaseQuery is large because we don't process all of it (stop when we have enough)
        /// </summary>
        internal List<Question> BooleanQueryWithExclusionsBloomFilterVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize, int skip)
        {
            var gcInfo = new GCCollectionInfo();
            var timer = Stopwatch.StartNew();

            TagByQueryLookup queryInfo = GetQueryTypeInfo(type);
            Func<Question, string> fieldSelector = GetFieldSelector(type);
            ThrowIfInvalidParameters(tag, pageSize, queryInfo);

            //int bloomFilterSize = 40 * 1000 * 1000; // million's, 40mil produces several False +ve's
            int bloomFilterSize = 100 * 1000 * 1000; // million's

#if DEBUG
            var bloomFilterCreationTimer = Stopwatch.StartNew();
            var bloomFilter = new SimpleBloomFilter(bloomFilterSize);
            bloomFilterCreationTimer.Stop();
            Console.WriteLine("Took {0} ({1:N2} ms) to create the bloom filter with {2:N0} bits ({3:N2} bytes)",
                              bloomFilterCreationTimer.Elapsed, bloomFilterCreationTimer.Elapsed.TotalMilliseconds, bloomFilterSize, bloomFilterSize / 8);
#else
            var bloomFilter = new SimpleBloomFilter(bloomFilterSize);
#endif

#if DEBUG
            //var tests = new[] { 1066589, 2793150, 364114, 910374 }; // These are the Question Id's NOT the array index ([]) values!!
            var tests = new[] {  192257,  616585,  53029, 158368 }; // These ARE the array index ([]) values
            var debugging = HashSetCache.Value;
#endif
            foreach (var excludedTag in excludedTags)
            {
                foreach (var qu in queryInfo[excludedTag])
                {
                    bloomFilter.Add(qu);
#if DEBUG
                    debugging.Add(qu);

                    if (tests.Contains(qu))
                    {
                        // It it's false, it's DEFINITELY false
                        // It it's true, it could really be false (false +ve)
                        var possiblyExists = bloomFilter.PossiblyExists(qu, debugInfo: true);
                        Console.WriteLine("Bloom Filter.PossiblyExists - {0,8} = {1} ****", qu, possiblyExists);
                        Console.WriteLine("  DebuggingHashSet.Contains - {0,8} = {1} ****", qu, debugging.Contains(qu));
                    }
#endif
                }
            }
            var baseQuery = queryInfo[tag];
#if DEBUG
            var result = 
                baseQuery.Where(b => 
                    {
                        var possiblyExists = bloomFilter.PossiblyExists(b);
                        if (possiblyExists == false)
                            return true; // we can use it

                        if (debugging.Contains(b) == false)
                        {
                            var qu = questions[b];
                            Console.WriteLine("FALSE +VE: {0,8}, PossiblyExists = {1}, debugging.Contains() = {2}, Id = {3,8}, Tags = {4}",
                                              b, possiblyExists, debugging.Contains(b), qu.Id, string.Join(",", qu.Tags));
                        }
                        return false; // we can't use it
                    })
#else
            var result = baseQuery.Where(b => bloomFilter.PossiblyExists(b) == false)
#endif
                                .Skip(skip)
                                .Take(pageSize)
                                .Select(i => questions[i])
                                .ToList();
            timer.Stop();
            gcInfo.UpdateCollectionInfo();

            Console.WriteLine("Base Query: {0}, there are {1:N0} Excluded Tags", tag, excludedTags.Count);
            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));
            using (SetConsoleColour(GetColorForTimespan(timer.Elapsed)))
            {
                Console.WriteLine("Boolean Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms) - BLOOM",
                                        type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            }
            //Console.WriteLine("Got {0} results, Bloom Filter contains {1:N0} items (some could be dupes), Truthiness {2:N2}", 
            //                  result.Count(), bloomFilter.NumberOfItems, bloomFilter.Truthiness);
            Console.WriteLine("Got {0} results, Bloom Filter contains {1:N0} items (some could be dupes)", result.Count(), bloomFilter.NumberOfItems);
            Console.WriteLine(gcInfo.ToString());
            //var formattedResults = result.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //Console.WriteLine("  {0}", string.Join("\n  ", formattedResults));
            Console.WriteLine();

#if DEBUG
            foreach (var item in tests)
            {
                var possiblyExists = bloomFilter.PossiblyExists(item, debugInfo: true);
                Console.WriteLine("Bloom Filter.PossiblyExists - {0,8} = {1}", item, possiblyExists);
                Console.WriteLine("  DebuggingHashSet.Contains - {0,8} = {1}", item, debugging.Contains(item));
                Console.WriteLine();
            }
            // When the values in "tests" represent Question Id
            //var testResults = tests.Select(t => questions.First(qu => qu.Id == t))
            //                       .Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            // When the values in "tests" represent array indexes, i.e. questions[x]
            var testResults = tests.Select(t => questions[t])
                                    .Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            Console.WriteLine("  {0}", string.Join("\n  ", testResults));
#endif

            return result;
        }

        private IDisposable SetConsoleColour(ConsoleColor newColour)
        {
            var originalColour = Console.ForegroundColor;
            Console.ForegroundColor = newColour;
            return new DisposableAction(() => Console.ForegroundColor = originalColour);
        }

        private ConsoleColor GetColorForTimespan(TimeSpan elapsed)
        {
            if (elapsed.TotalMilliseconds > 500)
                return ConsoleColor.Red;
            else if (elapsed.TotalMilliseconds > 400)
                return ConsoleColor.DarkYellow;
            return Console.ForegroundColor;
        }

        private Func<Question, string> GetFieldSelector(QueryType type)
        {
            Func<Question, string> fieldSelector;
            switch (type)
            {
                case QueryType.LastActivityDate:
                    fieldSelector = qu => qu.LastActivityDate.ToString();
                    break;
                case QueryType.CreationDate:
                    fieldSelector = qu => qu.CreationDate.ToString();
                    break;
                case QueryType.Score:
                    fieldSelector = qu => qu.Score.HasValue ? qu.Score.ToString() : "<null>";
                    break;
                case QueryType.ViewCount:
                    fieldSelector = qu => qu.ViewCount.HasValue ? qu.ViewCount.ToString() : "<null>";
                    break;
                case QueryType.AnswerCount:
                    fieldSelector = qu => qu.AnswerCount.HasValue ? qu.AnswerCount.ToString() : "<null>";
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Invalid query type {0}", (int)type));
            }
            return fieldSelector;
        }

        private void ThrowIfInvalidParameters(string tag, int pageSize, TagByQueryLookup queryInfo)
        {
            if (string.IsNullOrWhiteSpace(tag) || queryInfo.ContainsKey(tag) == false)
                throw new InvalidOperationException(string.Format("Invalid tag specified: {0}", tag ?? "<NULL>"));

            if (pageSize < 1 || pageSize > 250)
                throw new InvalidOperationException(string.Format("Invalid page size provided: {0}, only values from 1 to 250 are allowed", pageSize));
        }
    }
}
