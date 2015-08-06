using Shared;
using StackOverflowTagServer.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Threading;

using HashSet = StackOverflowTagServer.CLR.HashSet<int>;
using TagByQueryLookup = System.Collections.Generic.Dictionary<string, int[]>;

namespace StackOverflowTagServer.Querying
{
    internal class BaseQueryProcessor
    {
        protected readonly List<Question> questions;
        protected readonly Func<QueryType, TagByQueryLookup> GetQueryTypeInfo;

        internal BaseQueryProcessor(List<Question> questions, Func<QueryType, TagByQueryLookup> getQueryTypeInfo)
        {
            this.questions = questions;
            this.GetQueryTypeInfo = getQueryTypeInfo;
        }

        protected readonly Lazy<HashSet> HashSetCache = new Lazy<HashSet>(() =>
        {
            var hashSet = new HashSet(new IntComparer());
            var initialiseMethod = typeof(HashSet).GetMethod("Initialize", BindingFlags.NonPublic | BindingFlags.Instance);
            var exclusionCount = 8500000; // 8.5 million, more than enough, our data-set only has 7.9million questions!
            initialiseMethod.Invoke(hashSet, new object[] { exclusionCount });
            return hashSet;
        }, LazyThreadSafetyMode.ExecutionAndPublication);

        protected HashSet GetCachedHashSet(IEnumerable<int> populateHashSet)
        {
            var hashSet = HashSetCache.Value;
            hashSet.Clear();
            hashSet.UnionWith(populateHashSet);
            return hashSet;
        }

        protected HashSet GetCachedHashSet()
        {
            var hashSet = HashSetCache.Value;
            hashSet.Clear();
            return hashSet;
        }

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
    }
}
