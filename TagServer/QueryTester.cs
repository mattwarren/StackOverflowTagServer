using Shared;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace StackOverflowTagServer
{
    internal class QueryTester
    {
        private List<Question> questions;

        internal QueryTester(List<Question> questions)
        {
            this.questions = questions;
        }

        internal void TestAndOrNotQueries()
        {
            /// TODO When it's available, do this via the API

            //var tag1 = "c#";
            //var tag2 = ".net"; // "java"
            //var tagGroup1 = tagsByViewCount[tag1];
            //var tagGroup2 = tagsByViewCount[tag2];

            ////Use Intersect for AND, Union for OR and Except for NOT
            //var allIntersect = tagGroup1.Intersect(tagGroup2).Select(i => questions[i]).ToArray();
            //var intersectTimer = Stopwatch.StartNew();
            //var tagIntersect = tagGroup1.Intersect(tagGroup2).Take(10).Select(i => questions[i]).ToArray();
            //intersectTimer.Stop();
            //var allIntersectMatch = tagIntersect.All(qu => qu.Tags.Contains(tag1) && qu.Tags.Contains(tag2));
            //Console.WriteLine("Doing a Intersect (AND) of \"{0}\", \"{1}\" tags took {2:0.00} msecs - all match {3}",
            //            tag1, tag2, intersectTimer.Elapsed.TotalMilliseconds, allIntersectMatch);

            //intersectTimer = Stopwatch.StartNew();
            //var tagIntersect2 = tagGroup1.Intersect(tagGroup2).Skip(allIntersect.Count() - 10).Take(10).Select(i => questions[i]).ToArray();
            //intersectTimer.Stop();
            //var allIntersectMatch2 = tagIntersect.All(qu => qu.Tags.Contains(tag1) && qu.Tags.Contains(tag2));
            //Console.WriteLine("Doing a Intersect (Skip({0})) (AND) of \"{1}\", \"{2}\" tags took {3:0.00} msecs - all match {4}",
            //            allIntersect.Count() - 10, tag1, tag2, intersectTimer.Elapsed.TotalMilliseconds, allIntersectMatch2);

            ////Use Intersect for AND, Union for OR and Except for NOT
            //var unionTimer = Stopwatch.StartNew();
            //var tagUnion = tagGroup1.Union(tagGroup2).Take(10).Select(i => questions[i]).ToArray();
            //unionTimer.Stop();
            //var allUnionMatch = tagUnion.All(qu => qu.Tags.Contains(tag1) || qu.Tags.Contains(tag2));
            //Console.WriteLine("Doing a Union (OR) of \"{0}\", \"{1}\" tags took {2:0.00} msecs - all match {3}",
            //                tag1, tag2, unionTimer.Elapsed.TotalMilliseconds, allUnionMatch);

            ////Use Intersect for AND, Union for OR and Except for NOT
            //var exceptTimer = Stopwatch.StartNew();
            //var tagExcept = tagGroup1.Except(tagGroup2).Take(10).Select(i => questions[i]).ToArray();
            //exceptTimer.Stop();
            //var allExceptMatch = tagExcept.All(qu => qu.Tags.Contains(tag1) && (qu.Tags.Contains(tag2) == false));
            //Console.WriteLine("Doing a Except (NOT) of \"{0}\", \"{1}\" tags took {2:0.00} msecs - all match {3}\n",
            //                tag1, tag2, exceptTimer.Elapsed.TotalMilliseconds, allExceptMatch);
        }

        internal void TestQueries()
        {
            /// TODO When it's available, do this via the API

            //var csharpTags = tagsByViewCount["c#"];
            //var javaTags = tagsByViewCount["java"];

            //var mostRecentTimer2 = Stopwatch.StartNew();
            //var startPosn = 0;
            //var takeAmt = 10;
            //var mostRecentCSharp2 = new List<Question>(takeAmt);
            //for (int i = startPosn; i < startPosn + takeAmt; i++)
            //{
            //    mostRecentCSharp2.Add(questions[csharpTags[i]]);
            //}
            //mostRecentTimer2.Stop();
            //Console.WriteLine("2) Most Recent C# tags took {0}, ({1:0.00} ms) - NOT using LINQ, using foreach instead ({2} items)",
            //            mostRecentTimer2.Elapsed, mostRecentTimer2.Elapsed.TotalMilliseconds, mostRecentCSharp2.Count());

            ////Top 10 Most recently updated!! C# questions is just
            //var mostRecentTimer = Stopwatch.StartNew();
            //var mostRecentCSharp = csharpTags.Skip(0).Take(10).Select(x => questions[x]).ToList();
            //mostRecentTimer.Stop();
            //Console.WriteLine("1) Most Recent C# tags took {0}, ({1:0.00} ms) - Using LINQ ({2} items)",
            //            mostRecentTimer.Elapsed, mostRecentTimer.Elapsed.TotalMilliseconds, mostRecentCSharp.Count());

            //var lessRecentTimer = Stopwatch.StartNew();
            //var lessRecentCSharp = csharpTags.Skip(1000).Take(10).Select(x => questions[x]).ToList();
            //lessRecentTimer.Stop();
            //Console.WriteLine("1) Less Recent C# tags took {0}, ({1:0.00} ms) - Using LINQ ({2} items)",
            //            lessRecentTimer.Elapsed, lessRecentTimer.Elapsed.TotalMilliseconds, lessRecentCSharp.Count());

            //mostRecentTimer2 = Stopwatch.StartNew();
            //mostRecentCSharp2 = new List<Question>(takeAmt);
            //for (int i = startPosn; i < startPosn + takeAmt; i++)
            //{
            //    mostRecentCSharp2.Add(questions[csharpTags[i]]);
            //}
            //mostRecentTimer2.Stop();
            //Console.WriteLine("2) Most Recent C# tags took {0}, ({1:0.00} ms) - NOT using LINQ, using foreach instead ({2} items)",
            //            mostRecentTimer2.Elapsed, mostRecentTimer2.Elapsed.TotalMilliseconds, mostRecentCSharp2.Count());

            //var areEqual = mostRecentCSharp.ListEquals(mostRecentCSharp2, (qu1, qu2) =>
            //{
            //    if (qu1.CreationDate == qu2.CreationDate &&
            //        qu1.LastActivityDate == qu2.LastActivityDate &&
            //        qu1.Id == qu2.Id &&
            //        qu1.Tags.ListEquals(qu2.Tags, (t1, t2) => t1 == t2))
            //        return true;

            //    return false;
            //});
            //Console.WriteLine("When comparing LINQ with non-LINQ method, results are {0}\n", areEqual ? "the SAME" : "DIFFERENT");
        }
    }
}
