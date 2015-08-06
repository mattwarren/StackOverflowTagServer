using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;

namespace StackOverflowTagServer.Querying
{
    internal class QueryProcessor : BaseQueryProcessor
    {
        internal QueryProcessor(List<Question> questions, Func<QueryType, TagByQueryLookup> getQueryTypeInfo)
            : base(questions, getQueryTypeInfo)
        {
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

            using (Utils.SetConsoleColour(Utils.GetColorForTimespan(timer.Elapsed)))
            {
                Logger.Log("Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms)",
                           type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            }
            var formattedResults = result.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(", ", r.Tags)));
            Logger.Log("  {0}\n", string.Join("\n  ", formattedResults));

            return result;
        }

        internal List<Question> BooleanQueryWithExclusionsLINQVersion(QueryType type, string tag, IList<string> excludedTags, int pageSize, int skip)
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

            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));
            Logger.Log("Base Query: {0}, there are {1:N0} Excluded Tags", tag, excludedTags.Count);
            using (Utils.SetConsoleColour(Utils.GetColorForTimespan(timer.Elapsed)))
            {
                Logger.Log("Boolean Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms) - SLOW",
                           type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            }
            Logger.Log("Got {0} results", results.Count());
            Logger.Log(gcInfo.ToString());
            //var formattedResults = results.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //Log("  {0}", string.Join("\n  ", formattedResults));
            Logger.Log("");

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

            var baseHashSet = GetCachedHashSet(queryInfo[tag]);
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

            Logger.Log("Base Query: {0}, there are {1:N0} Excluded Tags", tag, excludedTags.Count);
            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));
            using (Utils.SetConsoleColour(Utils.GetColorForTimespan(timer.Elapsed)))
            {
                Logger.Log("Boolean Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms) - FAST",
                           type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            }
            Logger.Log("Got {0} results, {1:N0} items left in baseHashSet", results.Count(), baseHashSet.Count);
            Logger.Log(gcInfo.ToString());
            //var formattedResults = results.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //Log("  {0}", string.Join("\n  ", formattedResults));
            Logger.Log("");

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
            var exclusions = GetCachedHashSet();
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

            Logger.Log("Base Query: {0}, there are {1:N0} Excluded Tags", tag, excludedTags.Count);
            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));
            using (Utils.SetConsoleColour(Utils.GetColorForTimespan(timer.Elapsed)))
            {
                Logger.Log("Boolean Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms) - FAST ALT",
                           type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            }
            Logger.Log("Got {0} results ({1} in allResults), {2:N0} items in exclusions", results.Count(), allResults.Count, exclusions.Count);
            Logger.Log(gcInfo.ToString());
            //var formattedResults = results.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //Log("  {0}", string.Join("\n  ", formattedResults));
            Logger.Log("");

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
            Logger.Log("Took {0} ({1:N2} ms) to create the bloom filter with {2:N0} bits ({3:N2} bytes)",
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
                        Logger.Log("Bloom Filter.PossiblyExists - {0,8} = {1} ****", qu, possiblyExists);
                        Logger.Log("  DebuggingHashSet.Contains - {0,8} = {1} ****", qu, debugging.Contains(qu));
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
                            Logger.Log("FALSE +VE: {0,8}, PossiblyExists = {1}, debugging.Contains() = {2}, Id = {3,8}, Tags = {4}",
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

            Logger.Log("Base Query: {0}, there are {1:N0} Excluded Tags", tag, excludedTags.Count);
            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));
            using (Utils.SetConsoleColour(Utils.GetColorForTimespan(timer.Elapsed)))
            {
                Logger.Log("Boolean Query {0} against tag \"{1}\", pageSize = {2}, skip = {3}, took {4} ({5:N2} ms) - BLOOM",
                           type, tag, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            }
            //Log("Got {0} results, Bloom Filter contains {1:N0} items (some could be dupes), Truthiness {2:N2}",
            //    result.Count(), bloomFilter.NumberOfItems, bloomFilter.Truthiness);
            Logger.Log("Got {0} results, Bloom Filter contains {1:N0} items (some could be dupes)", result.Count(), bloomFilter.NumberOfItems);
            Logger.Log(gcInfo.ToString());
            //var formattedResults = result.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //Log("  {0}", string.Join("\n  ", formattedResults));
            Logger.Log("");

#if DEBUG
            foreach (var item in tests)
            {
                var possiblyExists = bloomFilter.PossiblyExists(item, debugInfo: true);
                Logger.Log("Bloom Filter.PossiblyExists - {0,8} = {1}", item, possiblyExists);
                Logger.Log("  DebuggingHashSet.Contains - {0,8} = {1}", item, debugging.Contains(item));
                Logger.Log("");
            }
            // When the values in "tests" represent Question Id
            //var testResults = tests.Select(t => questions.First(qu => qu.Id == t))
            //                       .Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            // When the values in "tests" represent array indexes, i.e. questions[x]
            var testResults = tests.Select(t => questions[t])
                                    .Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            Logger.Log("  {0}", string.Join("\n  ", testResults));
#endif

            return result;
        }
    }
}
