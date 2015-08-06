using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using HashSet = StackOverflowTagServer.CLR.HashSet<int>;
using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;

namespace StackOverflowTagServer.Querying
{
    internal class ComplexQueryProcessor : BaseQueryProcessor
    {
        internal ComplexQueryProcessor(List<Question> questions, Func<QueryType, TagByQueryLookup> getQueryTypeInfo)
            : base(questions, getQueryTypeInfo)
        {
        }

        internal QueryResult Query(QueryInfo info, CLR.HashSet<string> tagsToExclude = null)
        {
            var timer = Stopwatch.StartNew();
            TagByQueryLookup queryInfo = GetQueryTypeInfo(info.Type);
            ThrowIfInvalidParameters(info.Tag, info.PageSize, queryInfo);
            ThrowIfInvalidParameters(info.OtherTag, info.PageSize, queryInfo);

            var tagCounter = 0;
            var otherTagCounter = 0;
            var exclusionCounter = new CounterWrapper(initialValue: 0);
            IEnumerable<int> tag1Query = queryInfo[info.Tag].Select(t => { tagCounter++; return t; });
            IEnumerable<int> tag2Query = queryInfo[info.OtherTag].Select(t => { otherTagCounter++; return t; });
            IEnumerable<int> query = Enumerable.Empty<int>();
            switch (info.Operator)
            {
                //Use Intersect for AND, Union for OR and Except for NOT
                case "AND":
                    query = tag1Query.Intersect(tag2Query);
                    if (tagsToExclude != null)
                        query = AddExclusionsToQuery(query, tagsToExclude, exclusionCounter);
                    break;
                // TODO Complete this!!
                //case "AND-NOT":
                //    break;

                case "OR":
                    // NOTE: Union on it's own isn't correct, it uses seq1.Concat(seq2).Distinct(),
                    // so it pulls ALL items from seq1, before pulling ANY items from seq2
                    // TODO this has a small bug, we can get items out of order as we pull them thru in pairs
                    // if t2 has several items that are larger than t1, t1 will still come out first!!
                    // So algorithm needs to be:
                    //  1) pull the LARGEST value (from t1 or t2)
                    //  2) process this item
                    //  3) repeat 1) again
                    query = tag1Query.Zip(tag2Query, (t1, t2) => new[] { t1, t2 })
                                         .SelectMany(item => item)
                                         .Distinct();
                    if (tagsToExclude != null)
                        query = AddExclusionsToQuery(query, tagsToExclude, exclusionCounter);
                    break;
                case "OR-NOT": //"i.e. .net+or+jquery-"
                    query = tag1Query.Zip(queryInfo[TagServer.ALL_TAGS_KEY], (t1, t2) => new[] { t1, t2 })
                                         .SelectMany(item => item)
                                         .Except(tag2Query)
                                         .Distinct();
                    if (tagsToExclude != null)
                        query = AddExclusionsToQuery(query, tagsToExclude, exclusionCounter);
                    break;

                case "NOT":
                    query = tag1Query.Except(tag2Query);
                    if (tagsToExclude != null)
                        query = AddExclusionsToQuery(query, tagsToExclude, exclusionCounter);
                    break;

                default:
                    throw new InvalidOperationException(string.Format("Invalid operator specified: {0}", info.Operator ?? "<NULL>"));
            }

            var result = query.Skip(info.Skip)
                            .Take(info.PageSize)
                            .Select(i => questions[i])
                            .ToList();
            timer.Stop();

            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));

            Logger.Log("REGULAR  Boolean Query: \"{0}\" {1} \"{2}\", pageSize = {3:N0}, skip = {4:N0}, took {5} ({6:N2} ms) REGULAR",
                       info.Tag, info.Operator, info.OtherTag, info.PageSize, info.Skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            Logger.Log("Got {0:} results in total, tag1 QueryCounter = {1:N0}, tag2 QueryCounter = {1:N0}",
                       result.Count(), tagCounter, otherTagCounter);

            return new QueryResult
            {
                Questions = result,
                Counters = new Dictionary<string, int>
                {
                    { "TagCounter", tagCounter },
                    { "OtherTagCounter", otherTagCounter },
                    { "ExclusionCounter", exclusionCounter.Counter }
                }
            };
        }

        private IEnumerable<int> AddExclusionsToQuery(IEnumerable<int> query, CLR.HashSet<string> tagsToExclude, CounterWrapper exclusionCounter)
        {
            return query.Where(i =>
            {
                if (questions[i].Tags.All(t => tagsToExclude.Contains(t) == false))
                {
                    return true;
                }
                exclusionCounter.Counter++;
                return false;
            });
        }

        private class CounterWrapper
        {
            public CounterWrapper(int initialValue)
            {
                Counter = initialValue;
            }

            public int Counter { get; set; }
        }

        internal QueryResult QueryNoLINQ(QueryInfo info, CLR.HashSet<string> tagsToExclude = null)
        {
            var timer = Stopwatch.StartNew();
            TagByQueryLookup queryInfo = GetQueryTypeInfo(info.Type);
            ThrowIfInvalidParameters(info.Tag, info.PageSize, queryInfo);
            ThrowIfInvalidParameters(info.OtherTag, info.PageSize, queryInfo);

            ComplexQueryResult queryResult = null;
            switch (info.Operator)
            {
                case "AND":
                    queryResult = AndQuery(queryInfo[info.Tag], queryInfo[info.OtherTag], info.PageSize, info.Skip, tagsToExclude);
                    break;
                // TODO Complete this!!
                //case "AND-NOT":
                //    break;

                case "OR":
                    queryResult = OrQuery(queryInfo[info.Tag], queryInfo[info.OtherTag], info.PageSize, info.Skip, tagsToExclude);
                    break;
                case "OR-NOT": //"i.e. .net+or+jquery-"
                    queryResult = OrNotQuery(queryInfo[info.Tag], queryInfo[info.OtherTag], queryInfo[TagServer.ALL_TAGS_KEY], info.PageSize, info.Skip, tagsToExclude);
                    break;

                case "NOT":
                    queryResult = NotQuery(queryInfo[info.Tag], queryInfo[info.OtherTag], info.PageSize, info.Skip, tagsToExclude);
                    break;

                default:
                    throw new InvalidOperationException(string.Format("Invalid operator specified: {0}", info.Operator ?? "<NULL>"));
            }
            timer.Stop();

            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));

            Logger.Log("NO LINQ  Boolean Query: \"{0}\" {1} \"{2}\", pageSize = {3:N0}, skip = {4:N0}, took {5} ({6:N2} ms) NO LINQ",
                       info.Tag, info.Operator, info.OtherTag, info.PageSize, info.Skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            Logger.Log("Got {0:} results in total, baseQueryCounter = {1:N0}, itemsSkipped = {2:N0}, excludedCounter = {3:N0} ({4} tags to be excluded)",
                       queryResult.Results.Count(), queryResult.BaseQueryCounter, queryResult.ItemsSkipped,
                       queryResult.ExcludedCounter, tagsToExclude != null ? tagsToExclude.Count.ToString("N0") : "NO");

            return new QueryResult
            {
                Questions = queryResult.Results,
                Counters = new Dictionary<string, int>
                {
                    { "BaseQueryCounter", queryResult.BaseQueryCounter },
                    { "ItemsSkipped", queryResult.ItemsSkipped },
                    { "ExcludedCounter", queryResult.ExcludedCounter }
                }
            };
        }

        ComplexQueryResult AndQuery(int[] tag1Ids, int[] tag2Ids, int pageSize, int skip, CLR.HashSet<string> tagsToExclude = null)
        {
            var queryResult = new ComplexQueryResult { Results = new List<Question>(pageSize), BaseQueryCounter = 0, ItemsSkipped = 0, ExcludedCounter = 0 };

            // From https://github.com/ungood/EduLinq/blob/master/Edulinq/Intersect.cs#L28-L42
            var andHashSet = GetCachedHashSet(tag2Ids);
            foreach (var item in tag1Ids)
            {
                if (queryResult.Results.Count >= pageSize)
                    break;

                queryResult.BaseQueryCounter++;

                if (tagsToExclude != null && questions[item].Tags.Any(t => tagsToExclude.Contains(t)))
                {
                    queryResult.ExcludedCounter++;
                }
                else if (andHashSet.Contains(item))
                {
                    andHashSet.Remove(item);
                    if (queryResult.ItemsSkipped >= skip)
                        queryResult.Results.Add(questions[item]);
                    else
                        queryResult.ItemsSkipped++;
                }
            }

            return queryResult;
        }

        ComplexQueryResult OrQuery(int[] tag1Ids, int[] tag2Ids, int pageSize, int skip, CLR.HashSet<string> tagsToExclude = null)
        {
            var queryResult = new ComplexQueryResult { Results = new List<Question>(pageSize), BaseQueryCounter = 0, ItemsSkipped = 0, ExcludedCounter = 0 };

            // TODO this has a small bug, we can get items out of order as we pull them thru in pairs
            // if t2 has several items that are larger than t1, t1 will still come out first!!
            // So algorithm needs to be:
            //  1) pull the LARGEST value (from t1 or t2)
            //  2) process this item
            //  3) repeat 1) again
            // From http://referencesource.microsoft.com/#System.Core/System/Linq/Enumerable.cs,2b8d0f02389aab71
            var alreadySeen = GetCachedHashSet();
            using (IEnumerator<int> e1 = tag1Ids.AsEnumerable().GetEnumerator())
            using (IEnumerator<int> e2 = tag2Ids.AsEnumerable().GetEnumerator())
            {
                while (e1.MoveNext() && e2.MoveNext())
                {
                    if (queryResult.Results.Count >= pageSize)
                        break;

                    queryResult.BaseQueryCounter++;

                    if (tagsToExclude != null && questions[e1.Current].Tags.Any(t => tagsToExclude.Contains(t)))
                    {
                        queryResult.ExcludedCounter++;
                    }
                    else if (alreadySeen.Add(e1.Current))
                    {
                        if (queryResult.ItemsSkipped >= skip)
                            queryResult.Results.Add(questions[e1.Current]);
                        else
                            queryResult.ItemsSkipped++;
                    }

                    if (queryResult.Results.Count >= pageSize)
                        break;
                    // TODO should we be doing this here as well!!?!?!
                    //baseQueryCounter++;

                    if (tagsToExclude != null && questions[e2.Current].Tags.Any(t => tagsToExclude.Contains(t)))
                    {
                        queryResult.ExcludedCounter++;
                    }
                    else if (alreadySeen.Add(e2.Current))
                    {
                        if (queryResult.ItemsSkipped >= skip)
                            queryResult.Results.Add(questions[e2.Current]);
                        else
                            queryResult.ItemsSkipped++;
                    }
                }
            }

            return queryResult;
        }

        ComplexQueryResult OrNotQuery(int[] tag1Ids, int[] tag2Ids, int [] allTagIds, int pageSize, int skip, CLR.HashSet<string> tagsToExclude = null)
        {
            var queryResult = new ComplexQueryResult { Results = new List<Question>(pageSize), BaseQueryCounter = 0, ItemsSkipped = 0, ExcludedCounter = 0 };

            var orNotHashSet = GetCachedHashSet(tag2Ids);
            var seenBefore = new HashSet(); //TODO can't cache more that 1 HashSet per/thread!!
            using (IEnumerator<int> e1 = tag1Ids.AsEnumerable().GetEnumerator())
            using (IEnumerator<int> e2 = allTagIds.AsEnumerable().GetEnumerator())
            {
                while (e1.MoveNext() && e2.MoveNext())
                {
                    if (queryResult.Results.Count >= pageSize)
                        break;

                    queryResult.BaseQueryCounter++;

                    if (tagsToExclude != null && questions[e1.Current].Tags.Any(t => tagsToExclude.Contains(t)))
                    {
                        queryResult.ExcludedCounter++;
                    }
                    else if (orNotHashSet.Contains(e1.Current) == false && seenBefore.Add(e1.Current))
                    {
                        if (queryResult.ItemsSkipped >= skip)
                            queryResult.Results.Add(questions[e1.Current]);
                        else
                            queryResult.ItemsSkipped++;
                    }

                    if (queryResult.Results.Count >= pageSize)
                        break;
                    // TODO should we be doing this here as well!!?!?!
                    //baseQueryCounter++;

                    if (tagsToExclude != null && questions[e2.Current].Tags.Any(t => tagsToExclude.Contains(t)))
                    {
                        queryResult.ExcludedCounter++;
                    }
                    else if (orNotHashSet.Contains(e2.Current) == false && seenBefore.Add(e2.Current))
                    {
                        if (queryResult.ItemsSkipped >= skip)
                            queryResult.Results.Add(questions[e2.Current]);
                        else
                            queryResult.ItemsSkipped++;
                    }
                }
            }

            return queryResult;
        }

        ComplexQueryResult NotQuery(int[] tag1Ids, int[] tag2Ids, int pageSize, int skip, CLR.HashSet<string> tagsToExclude = null)
        {
            var queryResult = new ComplexQueryResult { Results = new List<Question>(pageSize), BaseQueryCounter = 0, ItemsSkipped = 0, ExcludedCounter = 0 };

            // https://github.com/ungood/EduLinq/blob/master/Edulinq/Except.cs#L26-L40
            var notHashSet = GetCachedHashSet(tag2Ids);
            foreach (var item in tag1Ids)
            {
                if (queryResult.Results.Count >= pageSize)
                    break;

                queryResult.BaseQueryCounter++;

                if (tagsToExclude != null && questions[item].Tags.Any(t => tagsToExclude.Contains(t)))
                {
                    queryResult.ExcludedCounter++;
                }
                else if (notHashSet.Add(item))
                {
                    if (queryResult.ItemsSkipped >= skip)
                        queryResult.Results.Add(questions[item]);
                    else
                        queryResult.ItemsSkipped++;
                }
            }

            return queryResult;
        }

        internal class ComplexQueryResult
        {
            internal List<Question> Results { get; set; }

            internal int BaseQueryCounter { get; set; }
            internal int ItemsSkipped { get; set; }
            internal int ExcludedCounter { get; set; }
        }
    }
}
