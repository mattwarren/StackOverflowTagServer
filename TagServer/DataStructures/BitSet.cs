using ProtoBuf;
using System;

namespace StackOverflowTagServer.DataStructures
{
    /// <summary>
    /// ABOUT:
    /// Helps with operations that rely on bit marking to indicate whether an item in the 
    /// collection should be added, removed, visited already, etc. 
    /// 
    /// BitSet doesn't allocate the array; you must pass in an array or ints allocated on the 
    /// stack or heap. ToIntArrayLength() tells you the int array size you must allocate. 
    /// 
    /// USAGE:
    /// Suppose you need to represent a bit array of length (i.e. logical bit array length)
    /// BIT_ARRAY_LENGTH. Then this is the suggested way to instantiate BitSet:
    /// ***************************************************************************
    /// int intArrayLength = BitSet.ToIntArrayLength(BIT_ARRAY_LENGTH);
    /// BitSet bitSet;
    /// if (intArrayLength less than stack alloc threshold)
    ///     int* m_arrayPtr = stackalloc int[intArrayLength];
    ///     bitSet = new BitSet(m_arrayPtr, intArrayLength);
    /// else
    ///     int[] m_arrayPtr = new int[intArrayLength];
    ///     bitSet = new BitSet(m_arrayPtr, intArrayLength);
    /// ***************************************************************************
    /// 
    /// IMPORTANT:
    /// The second ctor args, length, should be specified as the length of the int array, not
    /// the logical bit array. Because length is used for bounds checking into the int array,
    /// it's especially important to get this correct for the stackalloc version. See the code 
    /// samples above; this is the value gotten from ToIntArrayLength(). 
    /// 
    /// The length ctor argument is the only exception; for other methods -- MarkBit and 
    /// IsMarked -- pass in values as indices into the logical bit array, and it will be mapped
    /// to the position within the array of ints.
    /// 

    [ProtoContract(UseProtoMembersOnly = true, SkipConstructor = true)]
    unsafe internal class BitSet
    {
        private const byte MarkedBitFlag = 1;
        private const byte IntSize = 32;
        
        [ProtoMember(1)]
        // m_length of underlying int array (not logical bit array)
        private int m_length;
        
        // IsPacked means that ProtoBuf drasticially reduces the amount of disk space needed, for more info see
        // http://stackoverflow.com/questions/5211959/wasted-bytes-in-protocol-buffer-arrays/5214156#5214156        
        //[ProtoMember(2)]
        [ProtoMember(2, IsPacked = true)]
        // array of ints
        private int[] m_array;

        internal int[] InternalArray {  get { return m_array; } }

        /// <summary>
        /// Instantiates a BitSet with a heap alloc'd array of ints
        /// </summary>
        /// <param name="bitArray">int array to hold bits</param>
        /// <param name="length">length of int array</param>
        internal BitSet(int[] bitArray, int length)
        {
            m_array = bitArray;
            m_length = length;
        }

        /// <summary>
        /// Mark bit at specified position
        /// </summary>
        /// <param name="bitPosition"></param>
        [System.Security.SecuritySafeCritical]
        internal unsafe void MarkBit(int bitPosition)
        {                                                
            int bitArrayIndex = bitPosition / IntSize;
            if (bitArrayIndex < m_length && bitArrayIndex >= 0)
            {
                m_array[bitArrayIndex] |= (MarkedBitFlag << (bitPosition % IntSize));
            }
            else
            {
                throw new ArgumentOutOfRangeException("bitPosition", 
                            String.Format("Must be less than {0}, but was {1} (bitPosition:{2})", m_length, bitArrayIndex, bitPosition));
            }
        }

        /// <summary>
        /// Is bit at specified position marked?
        /// </summary>
        /// <param name="bitPosition"></param>
        /// <returns></returns>
        [System.Security.SecuritySafeCritical]
        internal unsafe bool IsMarked(int bitPosition)
        {
            int bitArrayIndex = bitPosition / IntSize;
            if (bitArrayIndex < m_length && bitArrayIndex >= 0)
            {
                return ((m_array[bitArrayIndex] & (MarkedBitFlag << (bitPosition % IntSize))) != 0);                
            }
            else
            {
                // return false??!?!?
                throw new ArgumentOutOfRangeException("bitPosition", 
                            String.Format("Must be less than {0}, but was {1} (bitPosition:{2})", m_length, bitArrayIndex, bitPosition));
            }
        }

        /// <summary>
        /// Returns a reference to the current instance ANDed with value.
        /// Code from And http://referencesource.microsoft.com/#mscorlib/system/collections/bitarray.cs,0a9d097e057af932 
        /// </summary>        
        internal BitSet And(BitSet value)
        {
            if (value == null)
                throw new ArgumentNullException("value");
            if (m_length != value.m_length)
                throw new ArgumentException(String.Format("Array length differ, this: {0}, value: {1}", m_length, value.m_length));

            for (int i = 0; i < m_array.Length; i++)
            {
                m_array[i] &= value.m_array[i];
            }

            //_version++;
            return this;
        }

        /// <summary>
        /// Returns a reference to the current instance ORed with value.
        /// Code from Or http://referencesource.microsoft.com/#mscorlib/system/collections/bitarray.cs,d6b98dd3d39e346e
        /// </summary>        
        internal BitSet Or(BitSet value)
        {
            if (value == null)
                throw new ArgumentNullException("value");
            if (m_length != value.m_length)
                throw new ArgumentException(String.Format("Array length differ, this: {0}, value: {1}", m_length, value.m_length));

            for (int i = 0; i < m_array.Length; i++)
            {
                m_array[i] |= value.m_array[i];
            }

            //_version++;
            return this;
        }

        /// <summary>
        /// Returns a reference to the current instance XORed with value.
        /// Code from Xor http://referencesource.microsoft.com/#mscorlib/system/collections/bitarray.cs,0a9d097e057af932
        /// </summary>        
        internal BitSet Xor(BitSet value)
        {
            if (value == null)
                throw new ArgumentNullException("value");
            if (m_length != value.m_length)
                throw new ArgumentException(String.Format("Array length differ, this: {0}, value: {1}", m_length, value.m_length));

            for (int i = 0; i < m_array.Length; i++)
            {
                m_array[i] ^= value.m_array[i];
            }

            //_version++;
            return this;
        }

        /// <summary>
        /// Inverts all the bit values. On/true bit values are converted to off/false. 
        /// Off/false bit values are turned on/true. The current instance is updated and returned.
        /// Code from Not http://referencesource.microsoft.com/#mscorlib/system/collections/bitarray.cs,e71a526d814e6d57
        /// </summary>
        /// <returns></returns>
        internal BitSet Not()
        {
            for (int i = 0; i < m_array.Length; i++)
            {
                m_array[i] = ~m_array[i];
            }

            //_version++;
            return this;
        }

        internal int[] CollectIndexes(bool flag, int skip, int take)
        {
            // Walk m_array, collecting the indexes of bit's that are set to "flag"
            // skip the first "skip" matches and keep going until we have "take" index values
            return new int[0];
        }

        /// <summary>
        /// How many ints must be allocated to represent n bits. Returns (n+31)/32, but 
        /// avoids overflow
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        internal static int ToIntArrayLength(int n)
        {
            return n > 0 ? ((n - 1) / IntSize + 1) : 0;
        }        
    }
}
