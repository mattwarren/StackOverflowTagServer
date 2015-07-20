using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using HashSet = StackOverflowTagServer.CLR.HashSet<int>;
//using HashSet = System.Collections.Generic.HashSet<int>;
using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;

namespace StackOverflowTagServer.Querying
{
    internal class ComplexQueryProcessor : BaseQueryProcessor
    {
        internal ComplexQueryProcessor(List<Question> questions, Func<QueryType, TagByQueryLookup> getQueryTypeInfo)
            : base(questions, getQueryTypeInfo)
        {
        }

        internal QueryResult Query(QueryType type, string tag1, string tag2, string @operator, int pageSize, int skip, CLR.HashSet<string> tagsToExclude = null)
        {
            var timer = Stopwatch.StartNew();
            TagByQueryLookup queryInfo = GetQueryTypeInfo(type);
            ThrowIfInvalidParameters(tag1, pageSize, queryInfo);
            ThrowIfInvalidParameters(tag2, pageSize, queryInfo);

            var tag1QueryCounter = 0;
            var tag2QueryCounter = 0;
            IEnumerable<int> tag1Query = queryInfo[tag1].Select(t => { tag1QueryCounter++; return t; });
            IEnumerable<int> tag2Query = queryInfo[tag2].Select(t => { tag2QueryCounter++; return t; });
            IEnumerable<int> query = Enumerable.Empty<int>();
            switch (@operator)
            {
                //Use Intersect for AND, Union for OR and Except for NOT
                case "AND":
                    query = tag1Query.Intersect(tag2Query);
                    if (tagsToExclude != null)
                        query = query.Where(i => questions[i].Tags.All(t => tagsToExclude.Contains(t) == false));
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
                        query = query.Where(i => questions[i].Tags.All(t => tagsToExclude.Contains(t) == false));
                    break;
                case "OR-NOT": //"i.e. .net+or+jquery-"
                    query = tag1Query.Zip(queryInfo[TagServer.ALL_TAGS_KEY], (t1, t2) => new[] { t1, t2 })
                                         .SelectMany(item => item)
                                         .Except(tag2Query)
                                         .Distinct();
                    if (tagsToExclude != null)
                        query = query.Where(i => questions[i].Tags.All(t => tagsToExclude.Contains(t) == false));
                    break;

                case "NOT":
                    query = tag1Query.Except(tag2Query);
                    if (tagsToExclude != null)
                        query = query.Where(i => questions[i].Tags.All(t => tagsToExclude.Contains(t) == false));
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Invalid operator specified: {0}", @operator ?? "<NULL>"));
            }

            var result = query.Skip(skip)
                            .Take(pageSize)
                            .Select(i => questions[i])
                            .ToList();
            timer.Stop();

            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));

            var msg1 = String.Format("REGULAR  Boolean Query: \"{0}\" {1} \"{2}\", pageSize = {3:N0}, skip = {4:N0}, took {5} ({6:N2} ms) REGULAR",
                                     tag1, @operator, tag2, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            Console.WriteLine(msg1);
            Trace.Write(msg1);

            var msg2 = String.Format("Got {0:} results in total, tag1 QueryCounter = {1:N0}, tag2 QueryCounter = {1:N0}",
                                     result.Count(), tag1QueryCounter, tag2QueryCounter);
            Console.WriteLine(msg2);
            Trace.Write(msg2);

            //Func<Question, string> fieldSelector = GetFieldSelector(type);
            //var formattedResults = result.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //Console.WriteLine("  {0}", string.Join("\n  ", formattedResults));
            //Console.WriteLine("\n");

            return new QueryResult
            {
                Questions = result,
                Counters = new Dictionary<string, int>
                {
                    { "TagCounter", tag1QueryCounter },
                    { "OtherTagCounter", tag2QueryCounter }
                }
            };
        }

        internal QueryResult QueryNoLINQ(QueryType type, string tag1, string tag2, string @operator, int pageSize, int skip, CLR.HashSet<string> tagsToExclude = null)
        {
            var timer = Stopwatch.StartNew();
            TagByQueryLookup queryInfo = GetQueryTypeInfo(type);
            ThrowIfInvalidParameters(tag1, pageSize, queryInfo);
            ThrowIfInvalidParameters(tag2, pageSize, queryInfo);

            ComplexQueryResult queryResult = null;
            switch (@operator)
            {
                case "AND":
                    queryResult = AndQuery(queryInfo[tag1], queryInfo[tag2], pageSize, skip, tagsToExclude);
                    break;
                // TODO Complete this!!
                //case "AND-NOT":
                //    break;

                case "OR":
                    queryResult = OrQuery(queryInfo[tag1], queryInfo[tag2], pageSize, skip, tagsToExclude);
                    break;
                case "OR-NOT": //"i.e. .net+or+jquery-"
                    queryResult = OrNotQuery(queryInfo[tag1], queryInfo[tag2], queryInfo[TagServer.ALL_TAGS_KEY], pageSize, skip, tagsToExclude);
                    break;

                case "NOT":
                    queryResult = NotQuery(queryInfo[tag1], queryInfo[tag2], pageSize, skip, tagsToExclude);
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Invalid operator specified: {0}", @operator ?? "<NULL>"));
            }
            timer.Stop();

            Results.AddData(timer.Elapsed.TotalMilliseconds.ToString("#.##"));

            var msg1 = String.Format("NO LINQ  Boolean Query: \"{0}\" {1} \"{2}\", pageSize = {3:N0}, skip = {4:N0}, took {5} ({6:N2} ms) NO LINQ",
                                     tag1, @operator, tag2, pageSize, skip, timer.Elapsed, timer.Elapsed.TotalMilliseconds);
            Console.WriteLine(msg1);
            Trace.Write(msg1);

            var msg2 = String.Format("Got {0:} results in total, baseQueryCounter = {1:N0}, itemsSkipped = {2:N0}, excludedCounter = {3:N0} ({4} tags to be excluded)",
                                     queryResult.Results.Count(), queryResult.BaseQueryCounter, queryResult.ItemsSkipped,
                                     queryResult.ExcludedCounter, tagsToExclude != null ? tagsToExclude.Count.ToString("N0") : "NO");
            Console.WriteLine(msg2);
            Trace.Write(msg2);

            //Func<Question, string> fieldSelector = GetFieldSelector(type);
            //var formattedResults = result.Select(r => string.Format("Id: {0,8}, {1}: {2,4}, Tags: {3}, ", r.Id, type, fieldSelector(r), string.Join(",", r.Tags)));
            //Console.WriteLine("  {0}", string.Join("\n  ", formattedResults));
            //Console.WriteLine("\n");

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
