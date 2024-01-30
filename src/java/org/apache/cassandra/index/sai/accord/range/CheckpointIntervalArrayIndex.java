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

package org.apache.cassandra.index.sai.accord.range;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import accord.utils.AsymmetricComparator;
import accord.utils.CheckpointIntervalArray;
import accord.utils.CheckpointIntervalArrayBuilder;
import accord.utils.CheckpointIntervalArrayBuilder.Accessor;
import accord.utils.SortedArrays;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static accord.utils.CheckpointIntervalArrayBuilder.Links.LINKS;
import static accord.utils.CheckpointIntervalArrayBuilder.Strategy.ACCURATE;

public class CheckpointIntervalArrayIndex
{
    private static final Accessor<Interval[], Interval, byte[]> LIST_INTERVAL_ACCESSOR = new Accessor<>()
    {
        @Override
        public boolean endInclusive(Interval[] intervals)
        {
            return true;
        }

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
    
    public static class SortedListWriter
    {
        private final int bytesPerKey, bytesPerValue;

        public SortedListWriter(int bytesPerKey, int bytesPerValue)
        {
            this.bytesPerKey = bytesPerKey;
            this.bytesPerValue = bytesPerValue;
        }

        public long write(IndexOutput out, Interval[] sortedIntervals, Callback callback) throws IOException
        {
            SAICodecUtils.writeHeader(out);
            long treeFilePointer = out.getFilePointer();
            // write header
            out.writeVInt(bytesPerKey);
            out.writeVInt(bytesPerValue);
            out.writeVInt(sortedIntervals.length);
            
            // write values
            callback.preWalk(sortedIntervals);
            int count = 0;
            for (var it : sortedIntervals)
            {
                validate(count, it);

                out.writeBytes(it.start, it.start.length);
                out.writeBytes(it.end, it.end.length);
                out.writeBytes(it.value, it.value.length);
                callback.onWrite(count, it);
                count++;
            }
            SAICodecUtils.writeFooter(out);
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
            default void preWalk(Interval[] sortedIntervals) throws IOException {}
            default void onWrite(int index, Interval interval) throws IOException {}
        }
    }

    public enum NoopCallback implements SortedListWriter.Callback
    {
        instance
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
                 IndexInput indexInput = IndexInputReader.create(reader))
            {
                SAICodecUtils.validate(indexInput);
                if (pos != -1)
                    indexInput.seek(pos);

                bytesPerKey = indexInput.readVInt();
                bytesPerValue = indexInput.readVInt();
                recordSize = bytesPerKey * 2 + bytesPerValue;
                count = indexInput.readVInt();
                firstRecordOffset = indexInput.getFilePointer();
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(fh);
                throw Throwables.unchecked(t);
            }
            //TODO (now, efficiency): store min/max value to quickly check if the query intersects this data
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(fh);
        }

        public enum SeekReason { BINARY_SEARCH, GET, SCAN }

        private boolean maybeSeek(IndexInput indexInput, Stats stats, SeekReason reason, long target) throws IOException
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

        public byte[] getRecord(IndexInput indexInput, Stats stats, SeekReason reason, byte[] recordBuffer, int pos) throws IOException
        {
            maybeSeek(indexInput, stats, reason, fileOffsetStart(pos));
            stats.bytesRead += recordBuffer.length;
            indexInput.readBytes(recordBuffer, 0, recordBuffer.length);
            return recordBuffer;
        }

        public byte[] readStart(IndexInput indexInput, Stats stats, byte[] keyBuffer, int pos) throws IOException
        {
            maybeSeek(indexInput, stats, SeekReason.GET, fileOffsetStart(pos));
            stats.bytesRead += keyBuffer.length;
            indexInput.readBytes(keyBuffer, 0, keyBuffer.length);
            return keyBuffer;
        }

        public byte[] readEnd(IndexInput indexInput, Stats stats, byte[] keyBuffer, int pos) throws IOException
        {
            maybeSeek(indexInput, stats, SeekReason.GET, fileOffsetEnd(pos));
            stats.bytesRead += keyBuffer.length;
            indexInput.readBytes(keyBuffer, 0, keyBuffer.length);
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

        public int binarySearch(IndexInput indexInput, Stats stats, byte[] recordBuffer, int from, int to, byte[] find, AsymmetricComparator<byte[], byte[]> comparator, SortedArrays.Search op) throws IOException
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
                        default: throw new IllegalStateException("Unknown search operation: " + op);
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
            buffer.end   = Arrays.copyOfRange(record, bytesPerKey, bytesPerKey * 2);
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
        private final IndexOutputWriter out;
        private final long offset;
        private final long position;
        private long length = -1;

        public CheckpointWriter(IndexOutputWriter out) throws IOException
        {
            this.out = out;
            this.offset = out.getFilePointer();
            SAICodecUtils.writeHeader(out);
            position = out.getFilePointer();
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
            out.writeVInt(c.maxScanAndCheckpointMatches);
            write(c.bounds);
            write(c.headers);
            write(c.lists);
        }

        private void write(int[] array) throws IOException
        {
            out.writeVInt(array.length);
            for (int i = 0; i < array.length; i++)
                out.writeVInt(array[i]);
        }

        @Override
        public void close() throws IOException
        {
            SAICodecUtils.writeFooter(out);
            length = out.getFilePointer() - offset;
            out.close();
        }
    }

    public static class CheckpointReader implements Closeable
    {
        private final FileHandle fh;
        private final int[] bounds, headers, lists;
        private final int maxScanAndCheckpointMatches;

        public CheckpointReader(FileHandle fh, long pos)
        {
            this.fh = fh;
            try (RandomAccessReader reader = fh.createReader();
                 IndexInput input = IndexInputReader.create(reader))
            {
                SAICodecUtils.validate(input);
                if (pos != -1)
                    input.seek(pos);

                maxScanAndCheckpointMatches = input.readVInt();
                bounds = readArray(input);
                headers = readArray(input);
                lists = readArray(input);
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(fh);
                throw Throwables.unchecked(t);
            }
        }

        private static int[] readArray(IndexInput input) throws IOException
        {
            int size = input.readVInt();
            int[] array = new int[size];
            for (int i = 0; i < size; i++)
                array[i] = input.readVInt();
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
        private final IndexDescriptor indexDescriptor;
        private final IndexIdentifier indexIdentifier;
        private final SortedListWriter writer;

        public SegmentWriter(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier,
                             int bytesPerKey, int bytesPerValue)
        {
            this.indexDescriptor = indexDescriptor;
            this.indexIdentifier = indexIdentifier;
            this.writer = new SortedListWriter(bytesPerKey, bytesPerValue);
        }

        public SegmentMetadata.ComponentMetadataMap writeCompleteSegment(Interval[] sortedIntervals) throws IOException
        {
            SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
            try (IndexOutput treeOutput = indexDescriptor.openPerIndexOutput(IndexComponent.CINTIA_SORTED_LIST, indexIdentifier, true);
                 CheckpointWriter checkpointWriter = new CheckpointWriter(indexDescriptor.openPerIndexOutput(IndexComponent.CINTIA_CHECKPOINTS, indexIdentifier, true)))
            {
                // The SSTable component file is opened in append mode, so our offset is the current file pointer.
                long sortedOffset = treeOutput.getFilePointer();
                long sortedPosition = writer.write(treeOutput, sortedIntervals, checkpointWriter);

                // If the treePosition is less than 0 then we didn't write any values out and the index is empty
                if (sortedPosition < 0)
                    return components;

                components.put(IndexComponent.CINTIA_SORTED_LIST, sortedPosition, sortedOffset, treeOutput.getFilePointer() - sortedOffset, Map.of());
                components.put(IndexComponent.CINTIA_CHECKPOINTS, checkpointWriter.position, checkpointWriter.offset, checkpointWriter.length, Map.of());
            }
            return components;
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
            var recordBuffer = new byte[reader.recordSize];
            var stats = new Stats();
            long startNanos = Clock.Global.nanoTime();
            try (IndexInput indexInput = IndexFileUtils.instance.openInput(reader.fh))
            {
                var buffer = new Interval();
                Accessor<IndexInput, byte[], byte[]> accessor = new Accessor<>()
                {
                    @Override
                    public boolean endInclusive(IndexInput indexInput)
                    {
                        return true;
                    }

                    @Override
                    public int size(IndexInput indexInput)
                    {
                        return reader.count;
                    }

                    @Override
                    public byte[] get(IndexInput indexInput, int index)
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
                    public byte[] start(IndexInput indexInput, int index)
                    {
                        try
                        {
                            return reader.readStart(indexInput, stats, keyBuffer, index);
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
                    public byte[] end(IndexInput indexInput, int index)
                    {
                        try
                        {
                            return reader.readEnd(indexInput, stats, keyBuffer, index);
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
                    public int binarySearch(IndexInput indexInput, int from, int to, byte[] find, AsymmetricComparator<byte[], byte[]> comparator, SortedArrays.Search op)
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

                searcher.forEach(start, end, (i1, i2, i3, i4, index) -> {
                    stats.matches++;
                    callback.accept(reader.copyTo(accessor.get(indexInput, index), buffer));
                }, (i1, i2, i3, i4, startIdx, endIdx) -> {
                    try
                    {
                        reader.maybeSeek(indexInput, stats, SortedListReader.SeekReason.SCAN, reader.fileOffsetStart(startIdx));
                        for (int i = startIdx; i < endIdx; i++)
                        {
                            stats.matches++;
                            stats.bytesRead += recordBuffer.length;
                            indexInput.readBytes(recordBuffer, 0, recordBuffer.length);
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
            int rc = ByteArrayUtil.compareUnsigned(start, 0, b.start, 0, start.length);
            if (rc == 0)
                rc = ByteArrayUtil.compareUnsigned(end, 0, b.end, 0, end.length);
            if (rc == 0)
                rc = ByteArrayUtil.compareUnsigned(value, 0, b.value, 0, value.length);
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
