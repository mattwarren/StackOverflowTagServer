using Shared;
using StackOverflowTagServer.CLR;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;

namespace StackOverflowTagServer.Querying
{
    internal class BaseQueryProcessor
    {
        protected readonly List<Question> questions;
        protected readonly Func<QueryType, TagByQueryLookup> GetTagByQueryLookup;

        internal BaseQueryProcessor(List<Question> questions, Func<QueryType, TagByQueryLookup> getTagByQueryLookup)
        {
            this.questions = questions;
            this.GetTagByQueryLookup = getTagByQueryLookup;
        }

        // 8.5 million is more than enough, our data-set only has 7.9 million questions!
        protected ThreadLocal<HashSetCache<int>> cache = new ThreadLocal<HashSetCache<int>>(() => new HashSetCache<int>(initialSize: 850000, comparer: new IntComparer()));
        protected ThreadLocal<HashSetCache<int>> secondCache = new ThreadLocal<HashSetCache<int>>(() => new HashSetCache<int>(initialSize: 850000, comparer: new IntComparer()));

        protected Func<Question, string> GetFieldSelector(QueryType type)
        {
            Func<Question, string> fieldSelector;
            switch (type)
            {
                case QueryType.LastActivityDate:
                    fieldSelector = qu => qu.LastActivityDate.ToString();
                    break;
                case QueryType.CreationDate:
                    fieldSelector = qu => qu.CreationDate.ToString();
                    break;
                case QueryType.Score:
                    fieldSelector = qu => qu.Score.HasValue ? qu.Score.Value.ToString("N0") : "<null>";
                    break;
                case QueryType.ViewCount:
                    fieldSelector = qu => qu.ViewCount.HasValue ? qu.ViewCount.Value.ToString("N0") : "<null>";
                    break;
                case QueryType.AnswerCount:
                    fieldSelector = qu => qu.AnswerCount.HasValue ? qu.AnswerCount.Value.ToString("N0") : "<null>";
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Invalid query type {0}", (int)type));
            }
            return fieldSelector;
        }

        protected void ThrowIfInvalidParameters(string tag, int pageSize, TagByQueryLookup queryInfo)
        {
            if (string.IsNullOrWhiteSpace(tag) || queryInfo.ContainsKey(tag) == false)
                throw new InvalidOperationException(string.Format("Invalid tag specified: {0}", tag ?? "<NULL>"));

            if (pageSize < 1 || pageSize > 250)
                throw new InvalidOperationException(string.Format("Invalid page size provided: {0}, only values from 1 to 250 are allowed", pageSize));
        }

        protected void PrintResults(IEnumerable<Question> questions, string info, QueryType queryType)
        {
            Logger.Log("RESULTS for \"{0}\":", info);
            var fieldFetcher = GetFieldSelector(queryType);
            foreach (var question in questions.Take(10))
            {
                Logger.Log("  Qu=[{0,9:N0}], {1}={2,10}, Tags= {3}",
                           question.Id, queryType, fieldFetcher(question), String.Join(", ", question.Tags));
            }
        }

        protected void PrintResults(IEnumerable<int> bits, Func<int, int> questionLookup, string info, QueryType queryType)
        {
            Logger.Log("RESULTS for \"{0}\":", info);
            var fieldFetcher = GetFieldSelector(queryType);
            foreach (var bit in bits.Take(10))
            {
                var questionId = questionLookup == null ? bit : questionLookup(bit);
                var question = questions[questionId];
                Logger.Log("  Bit=[{0,9:N0}] -> Qu=[{1,9:N0}], {2}={3,10}, Tags= {4}",
                           bit, questionId, queryType, fieldFetcher(question), String.Join(", ", question.Tags));
            }
        }
    }
}
