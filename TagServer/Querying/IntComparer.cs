using System.Collections.Generic;

namespace StackOverflowTagServer.Querying
{
    class IntComparer : IEqualityComparer<int>
    {
        public bool Equals(int x, int y)
        {
            return x.Equals(y);
        }

        public int GetHashCode(int obj)
        {
            return obj.GetHashCode();
        }
    }
}
