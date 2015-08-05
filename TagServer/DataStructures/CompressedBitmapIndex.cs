using ProtoBuf;
using System;
using System.Collections.Generic;

namespace StackOverflowTagServer.DataStructures
{
    [ProtoContract(UseProtoMembersOnly = true, SkipConstructor = true)]
    //internal class CompressedBitSet : AbstractBitSet
    public class CompressedBitmapIndex : IBitmapIndex
    {
        private const byte MarkedBitFlag = 1;
        private const byte IntSize = 32;

        private static int SerializationCounter = 0;

        [ProtoMember(1)]
        // length of underlying int array (not logical bit array)
        private int length;

        [ProtoMember(2)]
        private Dictionary<int, int> bitLookup;

        [ProtoMember(3)]
        private BitmapIndex allZeros;

        [ProtoMember(4)]
        private BitmapIndex allOnes;

        /// <summary>
        /// Instantiates a CompressedBitSet with a heap alloc'd array of ints
        /// </summary>
        /// <param name="length">length of int array</param>
        internal CompressedBitmapIndex(int length, int expectedFill = 1000)
        {
            this.length = length;
            bitLookup = new Dictionary<int, int>(expectedFill); // what's a good value here??
            var bitSetLength = BitmapIndex.ToIntArrayLength(length);

            var allZeroValues = new int[bitSetLength];
            for (var i = 0; i < allZeroValues.Length; i++)
                allZeroValues[i] = -1; // this will give us all the bits set to 1 (Two's complement)
            allZeros = new BitmapIndex(allZeroValues);
            // This add *alot* of overhead when creating 100's of Compressed BitSets!!!
            //for (var i = 0; i < length; i++)
            //{
            //    // Sanity check
            //    if (allZeros.IsMarked(i) == false)
            //        Console.WriteLine("allZeros: Error at posn " + i);
            //}

            var allOneValues = new int[bitSetLength];
            for (var i = 0; i < allOneValues.Length; i++)
                allOneValues[i] = 0;
            allOnes = new BitmapIndex(allOneValues);
            // This add *alot* of overhead when creating 100's of Compressed BitSets!!!
            //for (var i = 0; i < length; i++)
            //{
            //    // Sanity check
            //    if (allOnes.IsMarked(i))
            //        Console.WriteLine("allOnes: Error at posn " + i);
            //}
        }

        /// <summary>
        /// Mark bit at specified position
        /// </summary>
        /// <param name="bitPosition"></param>
        //internal override void MarkBit(int bitPosition)
        public void MarkBit(int bitPosition)
        {
            int bitArrayIndex = bitPosition / IntSize;
            if (bitArrayIndex < length && bitArrayIndex >= 0)
            {
                if (bitLookup.ContainsKey(bitArrayIndex) == false)
                    bitLookup.Add(bitArrayIndex, 0);
                bitLookup[bitArrayIndex] |= (MarkedBitFlag << (bitPosition % IntSize));

                allZeros.ClearBit(bitArrayIndex);
                //if (allZeros.IsMarked(bitArrayIndex))
                //    Console.WriteLine("ERROR clearing allZeros!");

                allOnes.ClearBit(bitArrayIndex);
                //if (allOnes.IsMarked(bitArrayIndex))
                //    Console.WriteLine("ERROR clearing allOnes!");
            }
            else
            {
                var msg = string.Format("Must be less than {0}, but was {1} (bitPosition:{2})", length, bitArrayIndex, bitPosition);
                throw new ArgumentOutOfRangeException("bitPosition", msg);
            }
        }

        /// <summary>
        /// Is bit at specified position marked?
        /// </summary>
        /// <param name="bitPosition"></param>
        /// <returns></returns>
        //internal override bool IsMarked(int bitPosition)
        public bool IsMarked(int bitPosition)
        {
            int bitArrayIndex = bitPosition / IntSize;
            if (bitArrayIndex < length && bitArrayIndex >= 0)
            {
                if (allZeros.IsMarked(bitArrayIndex))
                    return false;
                if (allOnes.IsMarked(bitArrayIndex))
                    return true;
                return ((bitLookup[bitArrayIndex] & (MarkedBitFlag << (bitPosition % IntSize))) != 0);
            }
            else
            {
                var msg = string.Format("Must be less than {0}, but was {1} (bitPosition:{2})", length, bitArrayIndex, bitPosition);
                throw new ArgumentOutOfRangeException("bitPosition", msg);
            }
        }

        // TODO move these callbacks to the base class?!?

        //// See http://stackoverflow.com/questions/14838379/protobuf-net-post-deserialization-handler/14839426#14839426
        //// and https://code.google.com/p/protobuf-net/wiki/Attributes
        //[ProtoBeforeSerialization]
        //private void OnSerializing()
        //{
        //    //if (SerializationCounter > 10000 || (SerializationCounter % 1000 == 0 && SerializationCounter > 0))
        //    //if (SerializationCounter % 1000 == 0 && SerializationCounter > 0)
        //    //    Console.Write("2) SerializING {0,12:N0} - bitLookup.Count={1:N0}", SerializationCounter, bitLookup.Count);
        //}

        //[ProtoAfterSerialization]
        //private void OnSerialized()
        //{
        //    //if (SerializationCounter > 10000 || (SerializationCounter % 1000 == 0 && SerializationCounter > 0))
        //    if (SerializationCounter % 1000 == 0 && SerializationCounter > 0)
        //        Console.WriteLine("2) SerializED {0,12:N0} - bitLookup.Count={1:N0}", SerializationCounter, bitLookup.Count);

        //    SerializationCounter++;
        //}
    }
}
