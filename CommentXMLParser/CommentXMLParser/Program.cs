using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using ProtoBuf;

namespace CommentXMLParser
{
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
            string inputUrl = @"D:\Source\__RavenDB__\SO Datadump - 20-04-2010\posts.xml";
            
            var filename = "Questions-NEW.bin";
            var recreate = false;
            if (recreate)
            {
                var timer = Stopwatch.StartNew();
                var questions = SimpleStreamAxis(inputUrl)
                    .Where(el => (string) el.Attribute("PostTypeId") == "1")
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
                    })
                    //.Take(1000)
                    .ToList();
                timer.Stop();

                Console.WriteLine("Took {0} ({1} ms) to process {2} items",
                                  timer.Elapsed, timer.ElapsedMilliseconds, questions.Count);

                foreach (var value in questions.Take(10))
                {
                    Console.WriteLine(value);
                }    
                
                if (File.Exists(filename))
                    File.Delete(filename);

                var fileWriteTimer = Stopwatch.StartNew();
                using (var file = File.Create(filename))
                {
                    //Serializer.Serialize(file, values.Where(x => x.AcceptedAnswerId == null).Take(100).ToList());
                    //Serializer.Serialize(file, values.Where(x => x.AnswerCount == null).Take(100).ToList());
                    Serializer.Serialize(file, questions);
                }
                fileWriteTimer.Stop();
                Console.WriteLine("Took {0} to serialise {1} items to the file", fileWriteTimer.Elapsed, questions.Count);
            }

            List<Question> rttQuestions;
            var fileReadTimer = Stopwatch.StartNew();
            using (var file = File.OpenRead(filename))
            {
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
