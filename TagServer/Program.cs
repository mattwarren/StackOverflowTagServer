using ProtoBuf;
using Shared;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace StackOverflowTagServer
{
    class Program
    {
        static void Main(string[] args)
        {
            // This data-set contains ~590,000 qu's, but it's a few years old.
            // Currently there are ~840,000 questions, so we are at ~70%
            //var numQuestions = 600 * 1000;
            List<Question> rawQuestions;

            var filename = @"C:\Users\warma11\Downloads\__Code__\StackOverflowTagServer-master\Questions.bin";
            var memoryBefore = GC.GetTotalMemory(true);
            var fileReadTimer = Stopwatch.StartNew();
            using (var file = File.OpenRead(filename))
            {
                rawQuestions = Serializer.Deserialize<List<Question>>(file);
            }
            fileReadTimer.Stop();
            var memoryAfter = GC.GetTotalMemory(true);

            Console.WriteLine("Took {0} to DE-serialise {1:N0} Stack Overflow Questions from the file, used {2:0.00} MB of memory\n",
                                fileReadTimer.Elapsed, rawQuestions.Count, (memoryAfter - memoryBefore) / 1024.0 / 1024.0);

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

            var tagServer = new TagServer(rawQuestions);
            //For DEBUGGING only, so we can test on a smaller sample
            //var tagServer = new TagServer(rawQuestions.Take(10 * 1000).ToList()); 

            var queryTester = new QueryTester(tagServer.Questions);
            queryTester.TestAndOrNotQueries();
            queryTester.TestQueries();

            tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 10, skip: 0);
            tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 10, skip: 9);
            tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 10, skip: 10);

            tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 100, skip: 10000);
            tagServer.Query(QueryType.LastActivityDate, "c#", pageSize: 100, skip: 1000000);

            tagServer.Query(QueryType.Score, ".net", pageSize: 6, skip: 95);
            tagServer.Query(QueryType.Score, ".net", pageSize: 6, skip: 100);
            tagServer.Query(QueryType.Score, ".net", pageSize: 6, skip: 105);

            Console.WriteLine("Finished, press <ENTER> to exit");
            Console.ReadLine();
        }
    }
}
