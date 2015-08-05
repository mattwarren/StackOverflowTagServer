using ProtoBuf;

namespace StackOverflowTagServer.DataStructures
{
    [ProtoContract(UseProtoMembersOnly = true, SkipConstructor = true)]
    [ProtoInclude(1, typeof(BitmapIndex))]
    [ProtoInclude(2, typeof(CompressedBitmapIndex))]
    internal interface IBitmapIndex
    {
        void MarkBit(int bitPosition);

        bool IsMarked(int bitPosition);
    }
}
