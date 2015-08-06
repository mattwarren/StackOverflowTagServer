using ProtoBuf;
using System;
using System.Diagnostics;
using System.IO;
using System.Text;

using TagByQueryLookupBitSet = System.Collections.Generic.Dictionary<string, Ewah.EwahCompressedBitArray>;

namespace StackOverflowTagServer
{
    internal static class Serialisation
    {
        private static Ewah.EwahCompressedBitArraySerializer bitMapIndexSerialiser = new Ewah.EwahCompressedBitArraySerializer();

        internal static void SerialiseToDisk<T>(string fileName, string folder, T item)
        {
            var timer = Stopwatch.StartNew();
            var filePath = Path.Combine(folder, fileName);
            using (var fileSteam = new FileStream(filePath, FileMode.Create))
            {
                Serializer.Serialize(fileSteam, item);
            }
            timer.Stop();

            var info = new FileInfo(filePath);
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to serialise: {2} Size: {3,6:N2} MB",
                                     timer.Elapsed, timer.ElapsedMilliseconds, fileName.PadRight(52), info.Length / 1024.0 / 1024.0);
        }

        internal static void SerialiseBitMapIndexToDisk(string fileName, string folder, TagByQueryLookupBitSet bitSet)
        {
            var timer = Stopwatch.StartNew();
            var filePath = Path.Combine(folder, fileName);
            using (var fileSteam = new FileStream(filePath, FileMode.Create))
            {
                foreach (var item in bitSet)
                {
                    var tagAsBytes = Encoding.UTF8.GetBytes(item.Key);

                    // length of EwahCompressedBitArray + length of Tag/String (in bytes) + length of any other markers
                    int lengthOfEntireRecord = item.Value.SizeInBytes + 12;  //EwahCompressedBitArray adds 12 bytes of headers
                    lengthOfEntireRecord += (4 + tagAsBytes.Length + 8); // string length + string + # of bits set
                    fileSteam.Write(BitConverter.GetBytes(lengthOfEntireRecord), 0, 4); // int is 32-bit, 4 bytes

                    fileSteam.Write(BitConverter.GetBytes(tagAsBytes.Length), 0, 4); // write length of the string (in bytes) out first
                    fileSteam.Write(tagAsBytes, 0, tagAsBytes.Length);

                    // Here we could write out the # of bits that have set, then use it as a sanity-check!?!?
                    fileSteam.Write(BitConverter.GetBytes(item.Value.GetCardinality()), 0, 8); // long is 64-bit, 8 bytes

                    bitMapIndexSerialiser.Serialize(fileSteam, item.Value);

                    //Logger.LogStartupMessage("Wrote Tag {0,20}, Bit Map: Cardinality = {1:N0}, SizeInBytes = {2:N0} (Record Size = {3:N0})",
                    //                         item.Key, item.Value.GetCardinality(), item.Value.SizeInBytes, lengthOfEntireRecord);
                }
            }
            timer.Stop();

            var info = new FileInfo(filePath);
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to serialise:    {2} Size: {3,6:N2} MB",
                                     timer.Elapsed, timer.ElapsedMilliseconds, fileName.PadRight(52), info.Length / 1024.0 / 1024.0);
        }

        internal static T DeserialiseFromDisk<T>(string fileName, string folder)
        {
            var timer = Stopwatch.StartNew();
            var filePath = Path.Combine(folder, fileName);
            T result = default(T);
            using (var fileSteam = new FileStream(filePath, FileMode.Create))
            {
                result = Serializer.Deserialize<T>(fileSteam);
            }
            timer.Stop();

            var info = new FileInfo(filePath);
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to DE-serialise: {2} Size: {3,6:N2} MB",
                                     timer.Elapsed, timer.ElapsedMilliseconds, fileName.PadRight(50), info.Length / 1024.0 / 1024.0);

            return result;
        }

        internal static Ewah.EwahCompressedBitArray SerialiseBitMapIndexFromDisk(string fileName, string folder)
        {
            var timer = Stopwatch.StartNew();
            var bitMapFilePath = Path.Combine(folder, fileName);
            var bitMap = default(Ewah.EwahCompressedBitArray);
            using (var fileSteam = new FileStream(bitMapFilePath, FileMode.Open))
            {
                while (true)
                {
                    byte[] buff = new byte[8];
                    fileSteam.Read(buff, 0, 4);
                    int recordLength = BitConverter.ToInt32(buff, 0);

                    var record = new byte[recordLength];
                    var bytesRead = fileSteam.Read(record, 0, recordLength);

                    if (bytesRead <= 0)
                        break;

                    if (bytesRead != recordLength)
                        Logger.LogStartupMessage("Error, Expected to read {0:N0} bytes (recordLength), but only read {1:N0}", recordLength, bytesRead);

                    var tagAsBytesLength = BitConverter.ToInt32(record, 0);
                    var tag = Encoding.UTF8.GetString(record, 4, tagAsBytesLength);
                    var cardinality = BitConverter.ToUInt64(record, 4 + tagAsBytesLength);

                    var recordBytesToSkip = 4 + tagAsBytesLength + 8;
                    bitMap = bitMapIndexSerialiser.Deserialize(new MemoryStream(record, recordBytesToSkip, record.Length - recordBytesToSkip));
                    //Logger.LogStartupMessage("Read Tag {0,20}, Bit Map: Cardinality = {1:N0}, SizeInBytes = {2:N0} (Record Size = {3:N0})",
                    //                         tag, bitMap.GetCardinality(), bitMap.SizeInBytes, recordLength);

                    if (cardinality != bitMap.GetCardinality())
                        Logger.LogStartupMessage("Error, Cardinality from BitMap = {0:N0}, Expected {1:N0}", bitMap.GetCardinality(), cardinality);

                    var diff = Math.Abs((recordLength - recordBytesToSkip) - (bitMap.SizeInBytes + 12)); // BitMap add 12 bytes into the record
                    if (diff != 0)
                        Logger.LogStartupMessage("Error, BitMap \"SizeInBytes + 12\" = {0:N0}, is meant to match \"recordLength - recordBytesToSkip\" = {1:N0} (diff = {2:N0})",
                                                 bitMap.SizeInBytes + 12, recordLength - recordBytesToSkip, diff);
                }
            }
            timer.Stop();

            var info = new FileInfo(bitMapFilePath);
            Logger.LogStartupMessage("Took {0} ({1,6:N0} ms) to DE-serialise: {2} Size: {3,6:N2} MB",
                                     timer.Elapsed, timer.ElapsedMilliseconds, fileName.PadRight(52), info.Length / 1024.0 / 1024.0);

            return bitMap;
        }
    }
}
