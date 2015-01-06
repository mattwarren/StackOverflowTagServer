using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace StackOverflowTagServer
{
    static class Results
    {
        private static StreamWriter stream;

        internal static void CreateNewFile(string filename)
        {
            var directory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var fileStream = new FileStream(Path.Combine(directory, filename),
                                    mode: FileMode.Create,
                                    access: FileAccess.ReadWrite,
                                    share: FileShare.ReadWrite);
            stream = new StreamWriter(fileStream);
        }

        internal static void AddHeaders(params string [] headers)
        {
            stream.WriteLine(string.Join(", ", headers));
            stream.Flush();
        }

        internal static void StartNewRow()
        {
            stream.WriteLine();
            stream.Flush();
        }

        internal static void AddData(string data)
        {
            stream.Write(data.EndsWith(",") ? data : data + ",");
        }

        internal static void CloseFile()
        {
            if (stream != null)
            {
                stream.Flush();
                stream.Close();
                stream.Dispose();
            }
        }
    }
}
