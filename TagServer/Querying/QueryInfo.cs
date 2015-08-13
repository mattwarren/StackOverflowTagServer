using StackOverflowTagServer.DataStructures;

namespace StackOverflowTagServer.Querying
{
    public class QueryInfo
    {
        public QueryInfo()
        {
            // Set some sensible defaults
            PageSize = 25;
            Skip = 0;
            Operator = "AND";

            UseLinq = false;
            UseBitMapIndexes = false;

            UseLeppieExclusions = false;
            DebugMode = false;
    }

        public QueryType Type { get; set; }
        public int PageSize { get; set; }
        public int Skip { get; set; }

        public string Tag { get; set; }
        public string OtherTag { get; set; }
        public string Operator { get; set; }

        public bool UseLinq { get; set; }
        public bool UseBitMapIndexes { get; set; }
        public bool UseLeppieExclusions { get; set; }
        public bool DebugMode { get; set; }
    }
}
