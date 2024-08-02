/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.accord;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import accord.utils.AsymmetricComparator;
import accord.utils.CheckpointIntervalArray;
import accord.utils.CheckpointIntervalArrayBuilder;
import accord.utils.CheckpointIntervalArrayBuilder.Accessor;
import accord.utils.SortedArrays;
import org.apache.cassandra.index.accord.IndexDescriptor.IndexComponent;
import org.apache.cassandra.io.util.ChecksumedRandomAccessReader;
import org.apache.cassandra.io.util.ChecksumedSequentialWriter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Throwables;

import static accord.utils.CheckpointIntervalArrayBuilder.Links.LINKS;
import static accord.utils.CheckpointIntervalArrayBuilder.Strategy.ACCURATE;

//TODO (now): Add support for variable length tokens; this is needed for Ordered partitioner (which we plan to support)
public class CheckpointIntervalArrayIndex
{
    private static final Accessor<Interval[], Interval, byte[]> LIST_INTERVAL_ACCESSOR = new Accessor<>()
    {
        @Override
        public int size(Interval[] intervals)
        {
            return intervals.length;
        }

        @Override
        public Interval get(Interval[] intervals, int index)
        {
            return intervals[index];
        }

        @Override
        public byte[] start(Interval[] intervals, int index)
        {
            return intervals[index].start;
        }

        @Override
        public byte[] start(Interval interval)
        {
            return interval.start;
        }

        @Override
        public byte[] end(Interval[] intervals, int index)
        {
            return intervals[index].end;
        }

        @Override
        public byte[] end(Interval interval)
        {
            return interval.end;
        }

        @Override
        public Comparator<byte[]> keyComparator()
        {
            return (a, b) -> ByteArrayUtil.compareUnsigned(a, 0, b, 0, a.length);
        }

        @Override
        public int binarySearch(Interval[] intervals, int from, int to, byte[] find, AsymmetricComparator<byte[], Interval> comparator, SortedArrays.Search op)
        {
            return SortedArrays.binarySearch(intervals, from, to, find, comparator, op);
        }
    };
    public static final Supplier<Checksum> CHECKSUM_SUPPLIER = CRC32C::new;

    //TODO (performance): rather than row structure, would column structure be better?  Sorted tokens tend to have prefix relationships
    // so could compress the data more.  The negative here is the binary search might cost more and the scan after the random access needs end + value...
    // Its also possible to do a hybrid structure where either start/end are column and the other one is row based...
    //TODO (performance): store min/max values so could filter based off metadata without having to walk the tree first?  This means that the metadata
    // doesn't need to be stored in-memory 100% of the time and only when a file "could" match.  The perf here would trade read costs for less memory.
    //TODO (fault tolerence): right now there is no checksumming outside of the header, so a corruption in the middle
    // could lead to weird behavior... since this structure is fixed lenght it "should" only lead to mismatches or binary
    // search going the wrong direction...
    //TODO (fault tolerence): maybe replace readStart/End with readRecord and extrat the value from there, this makes it so it would be trivial to add a checksum per-record.
    // Given the migration from SAI work, we can now remove the TableId from the data (16 bytes) so a 4 byte footer wouldn't be a big cost.  We also compute the checksum on read/write
    // right now, just ignore the value... the performance is currently better than with SAI (less overhead as we are not generic), so the checksumming costs are effectivally 0.
    public static class SortedListWriter
    {
        private final int bytesPerKey, bytesPerValue;

        public SortedListWriter(int bytesPerKey, int bytesPerValue)
        {
            this.bytesPerKey = bytesPerKey;
            this.bytesPerValue = bytesPerValue;
        }

        public long write(ChecksumedSequentialWriter out, Interval[] sortedIntervals, Callback callback) throws IOException
        {
            long treeFilePointer = out.getFilePointer();
            // write header
            out.resetChecksum(); // reset checksum so the header is isolated
            out.writeUnsignedVInt32(bytesPerKey);
            out.writeUnsignedVInt32(bytesPerValue);
            out.writeUnsignedVInt32(sortedIntervals.length);
            out.writeInt(out.getValue32AndResetChecksum());

            // write values
            callback.preWalk(sortedIntervals);
            int count = 0;
            for (var it : sortedIntervals)
            {
                validate(count, it);

                out.resetChecksum();
                out.write(it.start, 0, it.start.length);
                out.write(it.end, 0, it.end.length);
                out.write(it.value, 0, it.value.length);
                out.writeInt(out.getValue32());
                callback.onWrite(count, it);
                count++;
            }
            //TODO (now): don't need as this was here only for SAI.  Offset/position are the same now
            return count == 0 ? -1 : treeFilePointer;
        }

        private void validate(int numIntervals, Interval it)
        {
            if (it.start.length != bytesPerKey)
                throw new IllegalArgumentException("Interval " + numIntervals + "'s start value is size " + it.start.length + ", but expected " + bytesPerKey);
            if (it.end.length != bytesPerKey)
                throw new IllegalArgumentException("Interval " + numIntervals + "'s end value is size " + it.end.length + ", but expected " + bytesPerKey);
            if (it.value.length != bytesPerValue)
                throw new IllegalArgumentException("Interval " + numIntervals + "'s value is size " + it.value.length + ", but expected " + bytesPerValue);
        }

        public interface Callback
        {
            default void preWalk(Interval[] sortedIntervals) throws IOException
            {
            }

            default void onWrite(int index, Interval interval) throws IOException
            {
            }
        }
    }

    public static class SortedListReader implements Closeable
    {
        private final FileHandle fh;
        private final long firstRecordOffset;
        private final int bytesPerKey, bytesPerValue, recordSize;
        private final int count;

        public SortedListReader(FileHandle fh, long pos)
        {
            this.fh = fh;

            try (RandomAccessReader reader = fh.createReader();
                 var in = new ChecksumedRandomAccessReader(reader, CHECKSUM_SUPPLIER))
            {
                if (pos != -1)
                    in.seek(pos);

                bytesPerKey = in.readUnsignedVInt32();
                bytesPerValue = in.readUnsignedVInt32();
                recordSize = bytesPerKey * 2 + bytesPerValue + Integer.BYTES;
                count = in.readUnsignedVInt32();
                int actualChecksum = in.getValue32AndResetChecksum();
                int expectedChecksum = in.readInt();
                assert actualChecksum == expectedChecksum;
                firstRecordOffset = reader.getFilePointer();
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(fh);
                throw Throwables.unchecked(t);
            }
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(fh);
        }

        public enum SeekReason
        {BINARY_SEARCH, GET, SCAN}

        private boolean maybeSeek(ChecksumedRandomAccessReader indexInput, Stats stats, SeekReason reason, long target) throws IOException
        {
            if (indexInput.getFilePointer() != target)
            {
                indexInput.seek(target);
                switch (reason)
                {
                    case SCAN:
                        stats.seekForScan++;
                        break;
                    case GET:
                        stats.seekForGet++;
                        break;
                    case BINARY_SEARCH:
                        stats.seekForBinarySearch++;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown reason: " + reason);
                }
                return true;
            }
            return false;
        }

        public byte[] getRecord(ChecksumedRandomAccessReader indexInput, Stats stats, SeekReason reason, byte[] recordBuffer, int pos) throws IOException
        {
            maybeSeek(indexInput, stats, reason, fileOffsetStart(pos));
            return getCurrentRecord(indexInput, stats, recordBuffer);
        }

        public byte[] getCurrentRecord(ChecksumedRandomAccessReader indexInput, Stats stats, byte[] recordBuffer) throws IOException
        {
            stats.bytesRead += recordBuffer.length + Integer.BYTES;
            indexInput.resetChecksum();
            indexInput.readFully(recordBuffer, 0, recordBuffer.length);
            int actualChecksum = indexInput.getValue32();
            int expectedChecksum = indexInput.readInt();
            assert actualChecksum == expectedChecksum;
            return recordBuffer;
        }

        public byte[] readStart(ChecksumedRandomAccessReader indexInput, Stats stats, byte[] recordBuffer, byte[] keyBuffer, int pos) throws IOException
        {
            getRecord(indexInput, stats, SeekReason.GET, recordBuffer, pos);
            copyStart(recordBuffer, keyBuffer);
            return keyBuffer;
        }

        public byte[] readEnd(ChecksumedRandomAccessReader indexInput, Stats stats, byte[] recordBuffer, byte[] keyBuffer, int pos) throws IOException
        {
            getRecord(indexInput, stats, SeekReason.GET, recordBuffer, pos);
            copyEnd(recordBuffer, keyBuffer);
            return keyBuffer;
        }

        public byte[] copyStart(byte[] recordBuffer, byte[] keyBuffer)
        {
            System.arraycopy(recordBuffer, 0, keyBuffer, 0, keyBuffer.length);
            return keyBuffer;
        }

        public byte[] copyEnd(byte[] recordBuffer, byte[] keyBuffer)
        {
            System.arraycopy(recordBuffer, bytesPerKey, keyBuffer, 0, bytesPerKey);
            return keyBuffer;
        }

        public int binarySearch(ChecksumedRandomAccessReader indexInput, Stats stats, byte[] recordBuffer, int from, int to, byte[] find, AsymmetricComparator<byte[], byte[]> comparator, SortedArrays.Search op) throws IOException
        {
            int found = -1;
            while (from < to)
            {
                int i = (from + to) >>> 1;
                int c = comparator.compare(find, getRecord(indexInput, stats, SeekReason.BINARY_SEARCH, recordBuffer, i));
                if (c < 0)
                {
                    to = i;
                }
                else if (c > 0)
                {
                    from = i + 1;
                }
                else
                {
                    switch (op)
                    {
                        default:
                            throw new IllegalStateException("Unknown search operation: " + op);
                        case FAST:
                            return i;

                        case CEIL:
                            to = found = i;
                            break;

                        case FLOOR:
                            found = i;
                            from = i + 1;
                    }
                }
            }
            // return -(low + 1);  // key not found.
            return found >= 0 ? found : -1 - to;
        }

        public Interval copyTo(byte[] record, Interval buffer)
        {
            buffer.start = Arrays.copyOfRange(record, 0, bytesPerKey);
            buffer.end = Arrays.copyOfRange(record, bytesPerKey, bytesPerKey * 2);
            buffer.value = Arrays.copyOfRange(record, bytesPerKey * 2, record.length);
            return buffer;
        }

        private long fileOffsetStart(int offset)
        {
            if (offset >= count)
                throw new IndexOutOfBoundsException("Start is from (0, " + count + "]; attempted to access " + offset);
            return firstRecordOffset + (offset * recordSize);
        }

        private long fileOffsetEnd(int offset)
        {
            if (offset >= count)
                throw new IndexOutOfBoundsException("Start is from (0, " + count + "]; attempted to access " + offset);
            return firstRecordOffset + (offset * recordSize) + bytesPerKey;
        }

        private long fileOffsetValue(int offset)
        {
            if (offset >= count)
                throw new IndexOutOfBoundsException("Start is from (0, " + count + "]; attempted to access " + offset);
            return firstRecordOffset + (offset * recordSize) + bytesPerKey * 2;
        }
    }

    public static class CheckpointWriter implements SortedListWriter.Callback, Closeable
    {
        private final ChecksumedSequentialWriter out;
        private final long offset;
        private final long position; //TODO (now): don't need as this was here only for SAI.  Offset/position are the same now
        private long length = -1;

        public CheckpointWriter(ChecksumedSequentialWriter out)
        {
            this.out = out;
            this.offset = position = out.getFilePointer();
        }

        @Override
        public void preWalk(Interval[] sortedIntervals) throws IOException
        {
            class Checkpoints
            {
                final int[] bounds, headers, lists;
                final int maxScanAndCheckpointMatches;

                Checkpoints(int[] bounds, int[] headers, int[] lists, int maxScanAndCheckpointMatches)
                {
                    this.bounds = bounds;
                    this.headers = headers;
                    this.lists = lists;
                    this.maxScanAndCheckpointMatches = maxScanAndCheckpointMatches;
                }
            }
            var c = new CheckpointIntervalArrayBuilder<>(LIST_INTERVAL_ACCESSOR, sortedIntervals, ACCURATE, LINKS).build((ignore, bounds, headers, lists, max) -> new Checkpoints(bounds, headers, lists, max));
            out.resetChecksum(); // reset checksum so it only covers this metadata
            out.writeUnsignedVInt32(c.maxScanAndCheckpointMatches);
            write(c.bounds);
            write(c.headers);
            write(c.lists);
            out.writeInt(out.getValue32AndResetChecksum());
        }

        private void write(int[] array) throws IOException
        {
            out.writeUnsignedVInt32(array.length);
            for (int i = 0; i < array.length; i++)
                out.writeVInt32(array[i]);
        }

        @Override
        public void close() throws IOException
        {
            length = out.getFilePointer() - offset;
            out.close();
        }
    }

    //TODO (performance): the current format assumes random list access is cheap, which isn't true for a disk index.
    // This format was chosen as a place holder for now so we don't drift from the in-memory logic; in the original paper
    // a new sorted list is used for each checkpoint, which then makes the access a sequential scan rather than random access.
    public static class CheckpointReader implements Closeable
    {
        private final FileHandle fh;
        private final int[] bounds, headers, lists;
        private final int maxScanAndCheckpointMatches;

        public CheckpointReader(FileHandle fh, long pos)
        {
            this.fh = fh;
            try (RandomAccessReader reader = fh.createReader();
                 ChecksumedRandomAccessReader input = new ChecksumedRandomAccessReader(reader, CHECKSUM_SUPPLIER))
            {
                if (pos != -1)
                    input.seek(pos);

                input.resetChecksum(); // reset checksum so it only covers this metadata
                maxScanAndCheckpointMatches = input.readUnsignedVInt32();
                bounds = readArray(input);
                headers = readArray(input);
                lists = readArray(input);
                int actualChecksum = input.getValue32AndResetChecksum();
                int expectedChecksum = input.readInt();
                assert actualChecksum == expectedChecksum;
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(fh);
                throw Throwables.unchecked(t);
            }
        }

        private static int[] readArray(ChecksumedRandomAccessReader input) throws IOException
        {
            int size = input.readUnsignedVInt32();
            int[] array = new int[size];
            for (int i = 0; i < size; i++)
                array[i] = input.readVInt32();
            return array;
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(fh);
        }
    }

    public static class SegmentWriter
    {
        private final IndexDescriptor id;
        private final SortedListWriter writer;

        public SegmentWriter(IndexDescriptor id, int bytesPerKey, int bytesPerValue)
        {
            this.id = id;
            this.writer = new SortedListWriter(bytesPerKey, bytesPerValue);
        }

        public EnumMap<IndexComponent, Segment.ComponentMetadata> write(Interval[] sortedIntervals) throws IOException
        {
            EnumMap<IndexComponent, Segment.ComponentMetadata> metas = new EnumMap<>(IndexComponent.class);
            try (ChecksumedSequentialWriter treeOutput = ChecksumedSequentialWriter.open(id.fileFor(IndexComponent.CINTIA_SORTED_LIST), true, CHECKSUM_SUPPLIER);
                 CheckpointWriter checkpointWriter = new CheckpointWriter(ChecksumedSequentialWriter.open(id.fileFor(IndexComponent.CINTIA_CHECKPOINTS), true, CHECKSUM_SUPPLIER)))
            {
                // The SSTable component file is opened in append mode, so our offset is the current file pointer.
                long sortedOffset = treeOutput.getFilePointer();
                long sortedPosition = writer.write(treeOutput, sortedIntervals, checkpointWriter);

                // If the treePosition is less than 0 then we didn't write any values out and the index is empty
                if (sortedPosition < 0)
                    return metas;
                //TODO (now): currently does SAI header so offset isn't correct here and need position
                metas.put(IndexComponent.CINTIA_SORTED_LIST, new Segment.ComponentMetadata(sortedPosition, treeOutput.getFilePointer()));
                metas.put(IndexComponent.CINTIA_CHECKPOINTS, new Segment.ComponentMetadata(checkpointWriter.position, checkpointWriter.out.getFilePointer()));
            }
            return metas;
        }
    }

    public static class Stats
    {
        int seekForGet, seekForBinarySearch, seekForScan;
        long durationNs, bytesRead, matches;

        @Override
        public String toString()
        {
            return "Stats{" +
                   "seeks={Get=" + seekForGet +
                   ", BinarySearch=" + seekForBinarySearch +
                   ", Scan=" + seekForScan +
                   "}, bytesRead=" + bytesRead +
                   ", matches=" + matches +
                   ", duration_micro=" + TimeUnit.NANOSECONDS.toMicros(durationNs) +
                   '}';
        }
    }

    public static class SegmentSearcher implements Closeable
    {
        private final SortedListReader reader;
        private final CheckpointReader checkpoints;

        public SegmentSearcher(FileHandle sortedListFile,
                               long sortedListPosition,
                               FileHandle checkpointFile,
                               long checkpointPosition)
        {
            this.reader = new SortedListReader(sortedListFile, sortedListPosition);
            this.checkpoints = new CheckpointReader(checkpointFile, checkpointPosition);
        }

        public Stats intersects(byte[] start, byte[] end, Consumer<Interval> callback) throws IOException
        {
            var keyBuffer = new byte[reader.bytesPerKey];
            var recordBuffer = new byte[reader.recordSize - Integer.BYTES];
            var stats = new Stats();
            long startNanos = Clock.Global.nanoTime();
            try (ChecksumedRandomAccessReader indexInput = new ChecksumedRandomAccessReader(reader.fh.createReader(), CHECKSUM_SUPPLIER))
            {
                var buffer = new Interval();
                Accessor<ChecksumedRandomAccessReader, byte[], byte[]> accessor = new Accessor<>()
                {
                    @Override
                    public int size(ChecksumedRandomAccessReader indexInput)
                    {
                        return reader.count;
                    }

                    @Override
                    public byte[] get(ChecksumedRandomAccessReader indexInput, int index)
                    {
                        try
                        {
                            return reader.getRecord(indexInput, stats, SortedListReader.SeekReason.GET, recordBuffer, index);
                        }
                        catch (IOException e)
                        {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public byte[] start(ChecksumedRandomAccessReader indexInput, int index)
                    {
                        try
                        {
                            return reader.readStart(indexInput, stats, recordBuffer, keyBuffer, index);
                        }
                        catch (IOException e)
                        {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public byte[] start(byte[] bytes)
                    {
                        return reader.copyStart(bytes, keyBuffer);
                    }

                    @Override
                    public byte[] end(ChecksumedRandomAccessReader indexInput, int index)
                    {
                        try
                        {
                            return reader.readEnd(indexInput, stats, recordBuffer, keyBuffer, index);
                        }
                        catch (IOException e)
                        {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public byte[] end(byte[] bytes)
                    {
                        return reader.copyEnd(bytes, keyBuffer);
                    }

                    @Override
                    public Comparator<byte[]> keyComparator()
                    {
                        return (a, b) -> ByteArrayUtil.compareUnsigned(a, 0, b, 0, a.length);
                    }

                    @Override
                    public int binarySearch(ChecksumedRandomAccessReader indexInput, int from, int to, byte[] find, AsymmetricComparator<byte[], byte[]> comparator, SortedArrays.Search op)
                    {
                        try
                        {
                            return reader.binarySearch(indexInput, stats, recordBuffer, from, to, find, comparator, op);
                        }
                        catch (IOException e)
                        {
                            throw new UncheckedIOException(e);
                        }
                    }
                };
                var searcher = new CheckpointIntervalArray<>(accessor, indexInput, checkpoints.bounds, checkpoints.headers, checkpoints.lists, checkpoints.maxScanAndCheckpointMatches);

                searcher.forEachRange(start, end, (i1, i2, i3, i4, index) -> {
                    stats.matches++;
                    callback.accept(reader.copyTo(accessor.get(indexInput, index), buffer));
                }, (i1, i2, i3, i4, startIdx, endIdx) -> {
                    try
                    {
                        if (startIdx == endIdx)
                            return;
                        reader.maybeSeek(indexInput, stats, SortedListReader.SeekReason.SCAN, reader.fileOffsetStart(startIdx));
                        for (int i = startIdx; i < endIdx; i++)
                        {
                            stats.matches++;
                            reader.getCurrentRecord(indexInput, stats, recordBuffer);
                            callback.accept(reader.copyTo(recordBuffer, buffer));
                        }
                    }
                    catch (IOException e)
                    {
                        throw new UncheckedIOException(e);
                    }
                }, 0, 0, 0, 0, 0);
            }
            finally
            {
                stats.durationNs = Clock.Global.nanoTime() - startNanos;
            }
            return stats;
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(checkpoints);
            FileUtils.closeQuietly(reader);
        }
    }

    public static class Interval implements Comparable<Interval>
    {
        public byte[] start, end, value; // mutable to avoid allocating Interval for every element

        public Interval()
        {
        }

        public Interval(byte[] start, byte[] end, byte[] value)
        {
            this.start = start;
            this.end = end;
            this.value = value;
        }

        public Interval(Interval other)
        {
            this.start = other.start;
            this.end = other.end;
            this.value = other.value;
        }

        @Override
        public int compareTo(Interval b)
        {
            int rc = compareStart(b);
            if (rc == 0)
                rc = ByteArrayUtil.compareUnsigned(value, 0, b.value, 0, value.length);
            return rc;
        }

        public int compareStart(Interval b)
        {
            int rc = ByteArrayUtil.compareUnsigned(start, 0, b.start, 0, start.length);
            if (rc == 0)
                rc = ByteArrayUtil.compareUnsigned(end, 0, b.end, 0, end.length);
            return rc;
        }

        public int compareEnd(Interval b)
        {
            int rc = ByteArrayUtil.compareUnsigned(end, 0, b.end, 0, end.length);
            if (rc == 0)
                rc = ByteArrayUtil.compareUnsigned(start, 0, b.start, 0, start.length);
            return rc;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Interval interval = (Interval) o;
            return Arrays.equals(start, interval.start) && Arrays.equals(end, interval.end) && Arrays.equals(value, interval.value);
        }

        @Override
        public int hashCode()
        {
            int result = Arrays.hashCode(start);
            result = 31 * result + Arrays.hashCode(end);
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }

        public boolean intersects(byte[] start, byte[] end)
        {
            if (ByteArrayUtil.compareUnsigned(this.start, end) >= 0)
                return false;
            if (ByteArrayUtil.compareUnsigned(this.end, start) <= 0)
                return false;
            return true;
        }
    }
}
