using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace StackOverflowTagServer
{
    public static class Logger
    {
        private static readonly List<string> messages = new List<string>();
        public static List<string> Messages { get { return messages; } }

        internal static void Log(string format, params object[] args)
        {
            var msg = string.Format(format, args);
            Log(msg);
        }

        internal static void Log(string msg = "")
        {
            Console.WriteLine(msg);
            Trace.WriteLine(msg);
        }

        internal static void LogStartupMessage(string format, params object[] args)
        {
            var msg = string.Format(format, args);
            LogStartupMessage(msg);
        }

        internal static void LogStartupMessage(string msg = "")
        {
            Console.WriteLine(msg);
            Trace.WriteLine(msg);
            messages.Add(msg);
        }
    }
}
