using System;
using System.IO;
using System.Reflection;

namespace StackOverflowTagServer
{
    internal static class Results
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
            try
            {
                stream.WriteLine(string.Join(", ", headers));
                stream.Flush();
            }
            catch (ObjectDisposedException)
            {
                // swallow
            }
        }

        internal static void StartNewRow()
        {
            try
            {
                stream.WriteLine();
                stream.Flush();
            }
            catch (ObjectDisposedException)
            {
                // swallow
            }
        }

        internal static void AddData(string data)
        {
            if (stream == null)
                return;

            try
            {
                stream.Write(data.EndsWith(",") ? data : data + ",");
            }
            catch (ObjectDisposedException)
            {
                // swallow
            }
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
