using System.Collections.Generic;

namespace StackOverflowTagServer
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
