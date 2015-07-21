namespace StackOverflowTagServer.DataStructures
{
    internal abstract class AbstractBitSet
    {
        internal abstract void MarkBit(int bitPosition);

        internal abstract bool IsMarked(int bitPosition);
    }
}
