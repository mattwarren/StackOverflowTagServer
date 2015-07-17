using Shared;
using System;
using System.Collections.Generic;

namespace StackOverflowTagServer
{
    public class QueryResult
    {
        public List<Question> Questions { get; set; }

        public int Tag1QueryCounter { get; set; }

        public int Tag2QueryCounter { get; set; }
    }
}
