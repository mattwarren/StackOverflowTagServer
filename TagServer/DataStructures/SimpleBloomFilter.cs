using System;
using System.Collections;

namespace StackOverflowTagServer.DataStructures
{
    // From http://stackoverflow.com/questions/18553961/what-hash-function-should-i-use-for-a-bloom-filter-with-128-bit-keys/23382392#23382392
    internal class SimpleBloomFilter
    {
        public delegate int HashFunction(int input);

        private readonly BitArray bitArray;
        private readonly int size;
        private long counter;

        public SimpleBloomFilter(int size)
        {
            bitArray = new BitArray(size);
            this.size = size;
        }

        public long NumberOfItems { get { return counter; } }

        public double Truthiness { get { return (double)TrueBits() / bitArray.Count; } }

        public void Add(int item)
        {
            int index1 = Math.Abs(HashThomasWang(item)) % size;
            bitArray[index1] = true;

            int index2 = Math.Abs(HashFNV1a(item)) % size;
            bitArray[index2] = true;

            //int index3 = Math.Abs(item.GetHashCode()) % size;
            //bitArray[index3] = true;

            counter++;
        }

#if DEBUG
        public bool PossiblyExists(int item, bool debugInfo = false)
#else
        /// <summary>
        /// A bloom filter is basically a bitvector, where you set bits.
        /// If you want to figure out if an item exists,
        /// the bloom filter will give you a TRUE if the item possibly exists
        /// and a FALSE if the item for sure doesn't exist.
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool PossiblyExists(int item)
#endif
        {
            int index1 = Math.Abs(HashThomasWang(item)) % size;
#if DEBUG
            if (debugInfo)
                Logger.Log("HashThomasWang - {0,8} = {1,8}, bitArray[{1,8}] = {2}", item, index1, bitArray[index1]);
#endif
            if (bitArray[index1] == false)
                return false;

            int index2 = Math.Abs(HashFNV1a(item)) % size;
#if DEBUG
            if (debugInfo)
                Logger.Log("     HashFNV1a - {0,8} = {1,8}, bitArray[{1,8}] = {2}", item, index2, bitArray[index2]);
#endif
            if (bitArray[index2] == false)
                return false;

            //int index3 = Math.Abs(item.GetHashCode()) % size;
            //if (bitArray[index3] == false)
            //    return false;

            return true;  // this can be a false-positive
        }

        // Taken from https://gist.github.com/richardkundl/8300092#211
        // Hashes a 32-bit signed int using Thomas Wang's method v3.1 (http://www.concentric.net/~Ttwang/tech/inthash.htm).
        // Runtime is suggested to be 11 cycles.
        private static int HashThomasWang(int input)
        {
            uint x = (uint)input;
            unchecked
            {
                x = ~x + (x << 15); // x = (x << 15) - x- 1, as (~x) + y is equivalent to y - x - 1 in two's complement representation
                x = x ^ (x >> 12);
                x = x + (x << 2);
                x = x ^ (x >> 4);
                x = x * 2057; // x = (x + (x << 3)) + (x<< 11);
                x = x ^ (x >> 16);
                return (int)x;
            }
        }

        // Taken http://stackoverflow.com/questions/13974443/c-sharp-implementation-of-fnv-hash
        private static int HashFNV1a(int value)
        {
            var intConverter = (Int32Converter)value;
            int hash = FNVConstants.OffsetBasis;
            hash = (hash ^ intConverter.Byte1) * FNVConstants.Prime;
            hash = (hash ^ intConverter.Byte2) * FNVConstants.Prime;
            hash = (hash ^ intConverter.Byte3) * FNVConstants.Prime;
            hash = (hash ^ intConverter.Byte4) * FNVConstants.Prime;
            return hash;
        }

        private int TrueBits()
        {
            int output = 0;
            foreach (bool bit in bitArray)
            {
                if (bit == true)
                    output++;
            }
            return output;
        }
    }

    static class FNVConstants
    {
        public static readonly int OffsetBasis = unchecked((int)2166136261);
        public static readonly int Prime = 16777619;
    }
}
