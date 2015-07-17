using System;

namespace StackOverflowTagServer.DataStructures
{
    public interface IGCInfo
    {
        int Gen0 { get; }
        int Gen1 { get; }
        int Gen2 { get; }
    }

    internal class GCCollectionInfo
    {
        private class GCInfo : IGCInfo
        {
            public int Gen0 { get; private set; }
            public int Gen1 { get; private set; }
            public int Gen2 { get; private set; }

            // We want this to be protected, so that instances of GCInfo can't be created directly
            protected GCInfo(int gen0, int gen1, int gen2)
            {
                Gen0 = gen0;
                Gen1 = gen1;
                Gen2 = gen2;
            }
        }

        private class GCInfoInstance : GCInfo
        {
            // This class just exists so we can call the protected ctor of GCInfo
            public GCInfoInstance(int gen0, int gen1, int gen2)
                : base(gen0, gen1, gen2)
            {
            }
        }

        public IGCInfo InitialValues { get; private set; }
        public IGCInfo Count { get; private set; }

        private bool _collected = false;

        public GCCollectionInfo()
        {
            InitialValues = new GCInfoInstance(GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
            //Count = new GCInfo(InitialValues.Gen0, InitialValues.Gen1, InitialValues.Gen2);
        }

        public void UpdateCollectionInfo()
        {
            Count = new GCInfoInstance(
                GC.CollectionCount(0) - InitialValues.Gen0,
                GC.CollectionCount(1) - InitialValues.Gen1,
                GC.CollectionCount(2) - InitialValues.Gen2);

            _collected = true;
        }

        public override string ToString()
        {
            if (_collected == false)
                return "UpdateCollectionInfo() has not been called, no intermediateResults to display";

            return string.Format(
                "GC Collections - Gen0: {0}, Gen1: {1}, Gen2: {2}{3}",
                Count.Gen0, Count.Gen1, Count.Gen2,
                Count.Gen2 > 0 ? " ****" : string.Empty);
        }
    }
}
