using Shared;
using System.Collections.Generic;

namespace StackOverflowTagServer
{
    public class QueryResult
    {
        public List<Question> Questions { get; set; }

        public Dictionary<string, int> Counters { get; set; }
    }
}
