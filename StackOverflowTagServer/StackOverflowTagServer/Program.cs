using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using ProtoBuf;

namespace StackOverflowTagServer
{
    class Program
    {
        static void Main(string[] args)
        {
            var numQuestions = 2600000; //2.6 million, roughly the amt of qu's in SO
            //var numQuestions = 500000; // 1/2 a million
            var rawQuestions = new List<Question>(numQuestions);

            var filename = @"C:\Users\matt.warren\Desktop\StackOverflow Tag Server Project\Questions.bin";
            List<Question> rttQuestions;
            var fileReadTimer = Stopwatch.StartNew();
            using (var file = File.OpenRead(filename))
            {
                rttQuestions = Serializer.Deserialize<List<Question>>(file);
            }
            fileReadTimer.Stop();

            Console.WriteLine("Took {0} to DE-serialise {1} items from the file", fileReadTimer.Elapsed, rttQuestions.Count);

            rawQuestions = rttQuestions;

            //var tagsList = new List<string> { 
            //                                "c#", "java", "php", "javascript", 
            //                                "jquery", "android", "iphone", "asp.net", 
            //                                "c++", ".net", "python", "mysql",
            //                                "html", "objective-c", "ruby-on-rails", "sql",
            //                                "css", "c", "ios", "wpf",
            //                                "sql-server", "ruby", "xml", "ajax"
            //                            };

            //var minDate = new DateTime(2008, 1, 1);
            //var maxDate = new DateTime(2012, 1, 1);
            //var dateRange = maxDate - minDate;

            //var random = new Random();
            //var memoryBefore = GC.GetTotalMemory(true);
            //var timer = Stopwatch.StartNew();
            //for (int i = 0; i < numQuestions; i++)
            //{                
            //    rawQuestions.Add(new Question 
            //                    { 
            //                        Id = i,
            //                        CreationDate = minDate.AddSeconds(random.Next((int)dateRange.TotalSeconds)),
            //                        LastActivityDate = minDate.AddSeconds(random.Next((int)dateRange.TotalSeconds)),
            //                        Tags = GetRandomTags(tagsList, random)
            //                    });
            //}
            //timer.Stop();
            //var memoryAfter = GC.GetTotalMemory(true);

            //Console.WriteLine("Took {0} to create {1:N0} questions, using {2:0.00} MB of memory",
            //                timer.Elapsed, numQuestions, (memoryAfter - memoryBefore) / 1024.0 / 1024.0);

            var tagGroupingTimer = Stopwatch.StartNew();
            var tags = rawQuestions.Select(x => x.Tags).ToList();

            var groupedTags = rawQuestions.SelectMany(x => x.Tags)
                                        .ToLookup(x => x)
                                        .Select(x => new { Tag = x.Key, Count = x.Count()})
                                        .ToList();
            var totalCount = groupedTags.Sum(x => x.Count);
            var differentTags = groupedTags.Count;
            tagGroupingTimer.Stop();
            Console.WriteLine("Took {0} ({1} ms) to group all the tags\n", 
                        tagGroupingTimer.Elapsed, tagGroupingTimer.ElapsedMilliseconds);

            var tagServer = new TagServer(rawQuestions, groupedTags.ToDictionary(x => x.Tag, tag => tag.Count));

            Console.WriteLine("Finished, press <ENTER> to exit");
            Console.ReadLine();
        }

        //Borrowed from http://stackoverflow.com/a/48141/4500
        static IList<string> GetRandomTags(IList<string> taglist, Random rand)
        {
            int k = rand.Next(1, 4); // items to select (1, 2, or 3 tags)
            var selected = new List<string>(k);
            var needed = (double)k;
            var available = taglist.Count;
            while (selected.Count < k) 
            {
               if (rand.NextDouble() < needed / available) 
               {
                  selected.Add(taglist[available-1]);
                  needed--;
               }
               available--;
            }
            return selected;
        }
    }

    public class TagServer
    {
        int[] sortedByCreationDate;
        int[] sortedByLastUpdateDate;
        IList<Question> questions;

        public TagServer(IList<Question> questionsList, IDictionary<string, int> groupedTags)
        {
            questions = questionsList;
            var numQuestions = questions.Count;
            
            sortedByCreationDate = Enumerable.Range(0, numQuestions).ToArray();
            sortedByLastUpdateDate = Enumerable.Range(0, numQuestions).ToArray();

            var sortingTimer = Stopwatch.StartNew();
            Array.Sort(sortedByLastUpdateDate, CompareByLookingUpLastUpdateDate);
            Array.Sort(sortedByCreationDate, CompareByLookingUpCreationDateValue);
            sortingTimer.Stop();
            Console.WriteLine("Took {0} ({1} ms) to sort the 2 arrays\n", sortingTimer.Elapsed, sortingTimer.ElapsedMilliseconds);

            int [] csharpTags = new int[groupedTags["c#"]];
            int[] javaTags = new int[groupedTags["java"]];
            var csharpTagPosn = 0;
            var javaTagPosn = 0;

            //Have the following structure and populate it (Except we can't put this in a dictionary, as it's on the HEAP!!!!)
            var tagLists = new Dictionary<string, int []>(groupedTags.Count);
            foreach (var tagGroup in groupedTags)
            {
                tagLists.Add(tagGroup.Key, new int[tagGroup.Value]);
            }

            var filterTimer = Stopwatch.StartNew();
            for (int i = 0; i < sortedByLastUpdateDate.Length; i++)
            {
                var currentPosn = sortedByLastUpdateDate[i];
                var currentQuestion = questionsList[currentPosn];
                if (currentQuestion.Tags.Any(tag => tag == "c#"))
                {
                    csharpTags[csharpTagPosn] = currentPosn;
                    csharpTagPosn++;
                }
                if (currentQuestion.Tags.Any(tag => tag == "java"))
                {
                    javaTags[javaTagPosn] = currentPosn;
                    javaTagPosn++;
                }
            }
            filterTimer.Stop();
            Console.WriteLine("Filtering tag lists took {0} ({1} msecs) (just C# and Java)", 
                        filterTimer.Elapsed, filterTimer.ElapsedMilliseconds);
            
            //Use Intersect for AND, Union for OR and Except for NOT
            var intersectTimer = Stopwatch.StartNew();
            var tagIntersect = csharpTags.Intersect(javaTags).Take(200).ToArray();
            intersectTimer.Stop();
            var allIntersectMatch =
               tagIntersect.All(x =>
               {
                   var question = questionsList[x];
                   return question.Tags.Contains("c#") && question.Tags.Contains("java");
               });
            Console.WriteLine("Doing a Intersect (AND) of C#, Java tags took {0} msecs - all match {1}",
                        intersectTimer.ElapsedMilliseconds, allIntersectMatch);
           
            //Use Intersect for AND, Union for OR and Except for NOT
            var unionTimer = Stopwatch.StartNew();
            var tagUnion = csharpTags.Union(javaTags).Take(200).ToArray();
            unionTimer.Stop();
            var allUnionMatch =
                tagUnion.All(x =>
                {
                    var question = questionsList[x];
                    return question.Tags.Contains("c#") || question.Tags.Contains("java");
                });
            Console.WriteLine("Doing a Union (OR) of C#, Java tags took {0} msecs - all match {1}",
                        unionTimer.ElapsedMilliseconds, allUnionMatch);
      
            //Use Intersect for AND, Union for OR and Except for NOT
            var exceptTimer = Stopwatch.StartNew();
            var tagExcept = csharpTags.Except(javaTags).Take(200).ToArray();
            exceptTimer.Stop();
            var allExceptMatch =
               tagExcept.All(x =>
               {
                   var question = questionsList[x];
                   return question.Tags.Contains("c#") && (question.Tags.Contains("java") == false);
               });
            Console.WriteLine("Doing a Except (NOT) of C#, Java tags took {0} msecs - all match {1}",
                        exceptTimer.ElapsedMilliseconds, allExceptMatch);
           
            var tagsThatAppearWithCSharpTimer = Stopwatch.StartNew();
            var tagsThatAppearWithCSharpLookup = new Dictionary<string, int>();
            for (int i = 0; i < csharpTags.Length; i++)
            {
                var question = questionsList[csharpTags[i]];
                foreach (var tag in question.Tags)
                {
                    if (tag == "c#")
                        continue;

                    if (tagsThatAppearWithCSharpLookup.ContainsKey(tag))
                        tagsThatAppearWithCSharpLookup[tag]++;
                    else
                        tagsThatAppearWithCSharpLookup.Add(tag, 1);
                }
            }
            var tagsThatAppearWithCSharp = new List<Tuple<string, int>>(tagsThatAppearWithCSharpLookup.Count);
            foreach (var item in tagsThatAppearWithCSharpLookup.OrderByDescending(x => x.Value))
            {
                tagsThatAppearWithCSharp.Add(Tuple.Create(item.Key, item.Value));
            }
            tagsThatAppearWithCSharpTimer.Stop();
            Console.WriteLine("Took {0} ({1} ms) to generate tagsThatAppearWithCSharp list (Ordered by Count)",
                        tagsThatAppearWithCSharpTimer.Elapsed, tagsThatAppearWithCSharpTimer.ElapsedMilliseconds);

            //Top 10 Most recently updated!! C# questions is just
            var mostRecentTimer = Stopwatch.StartNew();
            var mostRecentCSharp = csharpTags.Skip(0).Take(10).Select(x => questionsList[x]).ToList();
            mostRecentTimer.Stop();
            Console.WriteLine("1) Most Recent C# tags took {0}, ({1} ms)",
                        mostRecentTimer.Elapsed, mostRecentTimer.ElapsedMilliseconds);

            var mostRecentTimer2 = Stopwatch.StartNew();
            var startPosn = 0;
            var takeAmt = 10;
            var mostRecentCSharp2 = new List<Question>(takeAmt);
            for (int i = startPosn; i < startPosn + takeAmt; i++)
            {
                mostRecentCSharp2.Add(questionsList[csharpTags[i]]);
            }
            mostRecentTimer2.Stop();
            Console.WriteLine("2) Most Recent C# tags took {0}, ({1} ms)", 
                        mostRecentTimer2.Elapsed, mostRecentTimer2.ElapsedMilliseconds);

            var areEqual = mostRecentCSharp.ListEquals(mostRecentCSharp2, (qu1, qu2) =>
                {
                    if (qu1.CreationDate == qu2.CreationDate &&
                        qu1.LastActivityDate == qu2.LastActivityDate &&
                        qu1.Id == qu2.Id &&
                        qu1.Tags.ListEquals(qu2.Tags, (t1, t2) => t1 == t2))
                        return true;

                    return false;
                });

            var csharpTagsCheck = csharpTags.All(x => questionsList[x].Tags.Contains("c#"));
            var javaTagsCheck = javaTags.All(x => questionsList[x].Tags.Contains("java"));

            var createdByDates = sortedByCreationDate.Select(x => questions[x].CreationDate).ToList();
            var results1 = sortedByCreationDate.SelectWithPrevious((prev, curr) =>
                {
                    if (questions[curr].CreationDate > questions[prev].CreationDate)
                    {
                        Console.WriteLine("CreationDate - [{0}] > [{1}]", curr, prev);
                        return false;
                    }
                    return true;
                }).ToList();

            var lastUpdatedDates = sortedByLastUpdateDate.Select(x => questions[x].LastActivityDate).ToList();
            var results2 = sortedByLastUpdateDate.SelectWithPrevious((prev, curr) =>
            {
                if (questions[curr].LastActivityDate > questions[prev].LastActivityDate)
                {
                    Console.WriteLine("[{0}] > [{1}]", curr, prev);
                    return false;
                }
                return true;
            }).ToList();
        }

        private int CompareByLookingUpCreationDateValue(int x, int y)
        {
            //return questions[x].CreationDate.CompareTo(questions[y].CreationDate);
            return questions[y].CreationDate.CompareTo(questions[x].CreationDate);
        }

        private int CompareByLookingUpLastUpdateDate(int x, int y)
        {
            //return questions[x].LastUpdateDate.CompareTo(questions[y].LastUpdateDate);
            return questions[y].LastActivityDate.CompareTo(questions[x].LastActivityDate);
        }
    } 
}
