using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace StackOverflowTagServer
{
    public static class Utils
    {
        public static List<string> GetLeppieTagsFromResource()
        {
            var leppieTags = new List<string>();
            var resourceStream = Utils.GetStream("leppie - excluded tags.txt");
            if (resourceStream != null)
            {
                var fileStream = new StreamReader(resourceStream);
                string line;
                while ((line = fileStream.ReadLine()) != null)
                    leppieTags.Add(line);
                //Logger.Log(string.Join(", ", tagsToExpand));
            }
            return leppieTags;
        }

        // From http://stackoverflow.com/questions/11590582/read-text-file-resource-from-net-library/11596483#11596483
        internal static Stream GetStream(string resourceName)
        {
            try
            {
                Assembly assy = Assembly.GetExecutingAssembly();
                string[] resources = assy.GetManifestResourceNames();
                for (int i = 0; i < resources.Length; i++)
                {
                    if (resources[i].ToLower().IndexOf(resourceName.ToLower()) != -1)
                    {
                        // resource found
                        return assy.GetManifestResourceStream(resources[i]);
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
            return Stream.Null;
        }

        internal static IDisposable SetConsoleColour(ConsoleColor newColour)
        {
            var originalColour = Console.ForegroundColor;
            Console.ForegroundColor = newColour;
            return new DisposableAction(() => Console.ForegroundColor = originalColour);
        }

        internal static ConsoleColor GetColorForTimespan(TimeSpan elapsed)
        {
            if (elapsed.TotalMilliseconds > 500)
                return ConsoleColor.Red;
            else if (elapsed.TotalMilliseconds > 400)
                return ConsoleColor.DarkYellow;
            return Console.ForegroundColor;
        }

        internal static void CompareLists(List<Question> listA, string nameA, List<Question> listB, string nameB)
        {
            if (listA.Count != listB.Count)
                Logger.LogStartupMessage("ERROR: list have different lengths, {0}: {1}, {2}: {3}", nameA, listA.Count, nameB, listB.Count);
            var AExceptB = listA.Select(r => r.Id).Except(listB.Select(r => r.Id)).ToList();
            if (AExceptB.Any())
            {
                Logger.LogStartupMessage("ERROR: Items in {0}, but not in {1}: {2}\n", nameA, nameB,
                                  string.Join(", ", AExceptB.Select(r => string.Format("[{0}]={1}", listA.FindIndex(s => s.Id == r), r))));
            }
            var BExceptA = listB.Select(r => r.Id).Except(listA.Select(r => r.Id)).ToList();
            if (BExceptA.Any())
            {
                Logger.LogStartupMessage("ERROR: Items in {0}, but not in {1}: {2}\n", nameB, nameA,
                                  string.Join(", ", BExceptA.Select(r => string.Format("[{0}]={1}", listB.FindIndex(s => s.Id == r), r))));
            }

            //foreach (var item in Enumerable.Range(0, Math.Min(listA.Count, listB.Count)))
            //{
            //    if (listA[item].Id != listB[item].Id)
            //        Logger.LogStartupMessage("ERROR: lists differ at position[{0}], {1} Id: {2}, {3} Id: {4}",
            //                          item, nameA, listA[item].Id, nameB, listB[item].Id);
            //}
        }

        internal static List<string> SelectNItemsFromList(List<string> expandedTags, int count)
        {
            if (count == expandedTags.Count)
                return expandedTags;
            else
            {
                var result = new List<string>(count);
                var step = expandedTags.Count / count;
                int i = 0;
                while (i < expandedTags.Count && (result.Count < count))
                {
                    result.Add(expandedTags[i]);
                    i += step;
                }
                return result;
            }
        }
    }
}
