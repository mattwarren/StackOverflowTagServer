﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using ProtoBuf;
using System.Collections.ObjectModel;

namespace CommentXMLParser
{
    public static class Extensions
    {
        public static IEnumerable<IEnumerable<T>> Partition<T>(this IEnumerable<T> source, int size)
        {
            T[] array = null;
            int count = 0;
            foreach (T item in source)
            {
                if (array == null)
                {
                    array = new T[size];
                }
                array[count] = item;
                count++;
                if (count == size)
                {
                    yield return new ReadOnlyCollection<T>(array);
                    array = null;
                    count = 0;
                }
            }
            if (array != null)
            {
                Array.Resize(ref array, count);
                yield return new ReadOnlyCollection<T>(array);
            }
        }
    }

    class Program
    {
        //From http://blogs.msdn.com/b/xmlteam/archive/2007/03/24/streaming-with-linq-to-xml-part-2.aspx
        static IEnumerable<XElement> SimpleStreamAxis(string inputUrl, string matchName = "")
        {
            using (XmlReader reader = XmlReader.Create(inputUrl))
            {
                reader.MoveToContent();
                while (reader.Read())
                {
                    switch (reader.NodeType)
                    {
                        case XmlNodeType.Element:
                            //if (reader.Name == matchName)
                            {
                                //XElement el = XElement.ReadFrom(reader) as XElement;
                                var el = XNode.ReadFrom(reader) as XElement;
                                if (el != null)
                                    yield return el;
                            }
                            break;
                    }
                }
                reader.Close();
            }
        }

        static void Main(string[] args)
        {
            string inputUrl = @"C:\Users\Matt\Downloads\Stack Exchange Data Dump - Sept 2013\Content\posts.xml";

            var filename = "Questions.bin";
            //var recreate = true; // false;
            var recreate = false;
            if (recreate)
            {
                //var timer = Stopwatch.StartNew();
                var questions = SimpleStreamAxis(inputUrl)
                    .Where(el => (string)el.Attribute("PostTypeId") == "1") // Answers are "2", everything else is "Other"
                    .Select(el =>
                    {
                        return new Question
                        {
                            Id = long.Parse((string)el.Attribute("Id")),
                            Title = (string)el.Attribute("Title"),
                            Tags = (string)el.Attribute("Tags"),
                            CreationDate =
                                DateTime.Parse((string)el.Attribute("CreationDate"), null, DateTimeStyles.RoundtripKind),
                            LastActivityDate =
                                DateTime.Parse((string)el.Attribute("LastActivityDate"), null, DateTimeStyles.RoundtripKind),
                            Score = ParseIntOrNullable((string)el.Attribute("Score")),
                            ViewCount = ParseIntOrNullable((string)el.Attribute("ViewCount")),
                            AnswerCount = ParseIntOrNullable((string)el.Attribute("AnswerCount")),
                            AcceptedAnswerId = ParseIntOrNullable((string)el.Attribute("AcceptedAnswerId"))
                        };
                    });

                if (File.Exists(filename))
                    File.Delete(filename);

                var timer = Stopwatch.StartNew();
                var questionCounter = 0;
                using (var file = File.Create(filename))
                {
                    var chunkCounter = 0;
                    foreach (var chunk in questions.Partition(100000))
                    {
                        Serializer.Serialize(file, chunk);
                        chunkCounter++;
                        var chunkSize = chunk.Count();
                        questionCounter += chunkSize;
                        Console.WriteLine("Wrote chunk {0,3}, with {1} items, {2,8} items so far", chunkCounter, chunkSize, questionCounter);
                    }
                }
                timer.Stop();
                Console.WriteLine("Took {0} to serialise {1} items to the file", timer.Elapsed, questionCounter);
            }

            List<Question> rttQuestions;
            var fileReadTimer = Stopwatch.StartNew();
            using (var file = File.OpenRead(filename))
            {
                //Serializer.Deserialize<ListQuestion
                rttQuestions = Serializer.Deserialize<List<Question>>(file);
            }
            fileReadTimer.Stop();
            
            Console.WriteLine("Took {0} to DE-serialise {1} items from the file", fileReadTimer.Elapsed, rttQuestions.Count);
            Console.WriteLine("Serialised file is {0}\n", HumanReadableFileSize(new FileInfo(filename).Length));

            //int questionsCount = 0, answersCount = 0, otherCount = 0;
            //foreach (var el in SimpleStreamAxis(inputUrl))
            //{
            //    var postTypeId = (string)el.Attribute("PostTypeId");
            //    if (postTypeId == "1")
            //        questionsCount++;
            //    else if (postTypeId == "2")
            //        answersCount++;
            //    else
            //        otherCount++;
            //}
            //Console.WriteLine("File contains {0} questions, {1} answers and {2} unknowns",
            //                  questionsCount, answersCount, otherCount);

            var orderingTimer = Stopwatch.StartNew();
            var mostViews = rttQuestions.OrderByDescending(x => x.ViewCount).Take(100).ToList();
            orderingTimer.Stop();
            Console.WriteLine("Took {0} to get the top 100 most popular pages (by view count)", orderingTimer.Elapsed);
            var mostViewsPages = mostViews.First();
        }

        private static string HumanReadableFileSize(long numBytes)
        {
            string[] sizes = { "B", "KB", "MB", "GB" };
            int order = 0;
            while (numBytes >= 1024 && order + 1 < sizes.Length)
            {
                order++;
                numBytes = numBytes / 1024;
            }
            string result = String.Format("{0:0.##} {1}", numBytes, sizes[order]);
            return result;
        }

        private static int? ParseIntOrNullable(string text)
        {
            if (String.IsNullOrEmpty(text))
                return null;

            return int.Parse(text);
        }

        [ProtoContract]
        public class Question
        {
            [ProtoMember(1)]
            public long Id { get; set; }
            [ProtoMember(2)]
            public string Title { get; set; }
            [ProtoMember(3)]
            public string Tags { get; set; }
            [ProtoMember(4)]
            public DateTime CreationDate { get; set; }
            [ProtoMember(5)]
            public DateTime LastActivityDate { get; set; }
            [ProtoMember(6)]
            public int? Score { get; set; }
            [ProtoMember(7)]
            public int? ViewCount { get; set; }
            [ProtoMember(8)]
            public int? AnswerCount { get; set; }
            [ProtoMember(9)]
            public int? AcceptedAnswerId { get; set; }
        }
    }
}
