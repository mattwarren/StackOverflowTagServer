using StackOverflowTagServer.DataStructures;

namespace StackOverflowTagServer.Querying
{
    public class QueryInfo
    {
        public QueryType Type { get; set; }
        public int PageSize { get; set; }
        public int Skip { get; set; }

        public string Tag { get; set; }
        public string OtherTag { get; set; }
        public string Operator { get; set; }

        public bool UseLinq { get; set; }
        public bool UseLeppieExclusions { get; set; }

        public bool DebugMode { get; set; }
    }
}
