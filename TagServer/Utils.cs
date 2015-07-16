using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace StackOverflowTagServer
{
    public static class Utils
    {
        // From http://stackoverflow.com/questions/11590582/read-text-file-resource-from-net-library/11596483#11596483
        public static Stream GetStream(string resourceName)
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
                //Console.WriteLine(string.Join(", ", tagsToExpand));
            }
            return leppieTags;
        }
    }
}
