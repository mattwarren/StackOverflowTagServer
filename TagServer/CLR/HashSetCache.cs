using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;

namespace StackOverflowTagServer.CLR
{
    internal class HashSetCache<T>
    {
        private readonly Lazy<StackOverflowTagServer.CLR.HashSet<T>> LazyHashSetCache;

        internal HashSetCache(int initialSize, IEqualityComparer<T> comparer = null)
        {
            LazyHashSetCache = new Lazy<StackOverflowTagServer.CLR.HashSet<T>>(() =>
                {
                    HashSet<T> hashSet;
                    hashSet = comparer == null ? new HashSet<T>() : new HashSet<T>(comparer);
                    var initialiseMethod = typeof(HashSet<T>).GetMethod("Initialize", BindingFlags.NonPublic | BindingFlags.Instance);
                    var exclusionCount = initialSize;
                    initialiseMethod.Invoke(hashSet, new object[] { exclusionCount });
                    return hashSet;
                }, LazyThreadSafetyMode.ExecutionAndPublication);
        }

        internal StackOverflowTagServer.CLR.HashSet<T> GetCachedHashSet(IEnumerable<T> populateHashSet)
        {
            var hashSet = LazyHashSetCache.Value;
            hashSet.Clear();
            hashSet.UnionWith(populateHashSet);
            return hashSet;
        }

        internal StackOverflowTagServer.CLR.HashSet<T> GetCachedHashSet()
        {
            var hashSet = LazyHashSetCache.Value;
            hashSet.Clear();
            return hashSet;
        }
    }
}
