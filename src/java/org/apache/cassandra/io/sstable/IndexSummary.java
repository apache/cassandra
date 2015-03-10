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
package org.apache.cassandra.io.sstable;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.WrappedSharedCloseable;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;

/*
 * Layout of Memory for index summaries:
 *
 * There are two sections:
 *  1. A "header" containing the offset into `bytes` of entries in the summary summary data, consisting of
 *     one four byte position for each entry in the summary.  This allows us do simple math in getIndex()
 *     to find the position in the Memory to start reading the actual index summary entry.
 *     (This is necessary because keys can have different lengths.)
 *  2.  A sequence of (DecoratedKey, position) pairs, where position is the offset into the actual index file.
 */
public class IndexSummary extends WrappedSharedCloseable
{
    public static final IndexSummarySerializer serializer = new IndexSummarySerializer();

    /**
     * A lower bound for the average number of partitions in between each index summary entry. A lower value means
     * that more partitions will have an entry in the index summary when at the full sampling level.
     */
    private final int minIndexInterval;

    private final IPartitioner partitioner;
    private final int sizeAtFullSampling;
    // we permit the memory to span a range larger than we use,
    // so we have an accompanying count and length for each part
    // we split our data into two ranges: offsets (indexing into entries),
    // and entries containing the summary data
    private final Memory offsets;
    private final int offsetCount;
    // entries is a list of (partition key, index file offset) pairs
    private final Memory entries;
    private final long entriesLength;

    /**
     * A value between 1 and BASE_SAMPLING_LEVEL that represents how many of the original
     * index summary entries ((1 / indexInterval) * numKeys) have been retained.
     *
     * Thus, this summary contains (samplingLevel / BASE_SAMPLING_LEVEL) * ((1 / indexInterval) * numKeys)) entries.
     */
    private final int samplingLevel;

    public IndexSummary(IPartitioner partitioner, Memory offsets, int offsetCount, Memory entries, long entriesLength,
                        int sizeAtFullSampling, int minIndexInterval, int samplingLevel)
    {
        super(new Memory[] { offsets, entries });
        assert offsets.getInt(0) == 0;
        this.partitioner = partitioner;
        this.minIndexInterval = minIndexInterval;
        this.offsetCount = offsetCount;
        this.entriesLength = entriesLength;
        this.sizeAtFullSampling = sizeAtFullSampling;
        this.offsets = offsets;
        this.entries = entries;
        this.samplingLevel = samplingLevel;
    }

    private IndexSummary(IndexSummary copy)
    {
        super(copy);
        this.partitioner = copy.partitioner;
        this.minIndexInterval = copy.minIndexInterval;
        this.offsetCount = copy.offsetCount;
        this.entriesLength = copy.entriesLength;
        this.sizeAtFullSampling = copy.sizeAtFullSampling;
        this.offsets = copy.offsets;
        this.entries = copy.entries;
        this.samplingLevel = copy.samplingLevel;
    }

    // binary search is notoriously more difficult to get right than it looks; this is lifted from
    // Harmony's Collections implementation
    public int binarySearch(RowPosition key)
    {
        ByteBuffer hollow = MemoryUtil.getHollowDirectByteBuffer();
        int low = 0, mid = offsetCount, high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            fillTemporaryKey(mid, hollow);
            result = -DecoratedKey.compareTo(partitioner, hollow, key);
            if (result > 0)
            {
                low = mid + 1;
            }
            else if (result == 0)
            {
                return mid;
            }
            else
            {
                high = mid - 1;
            }
        }

        return -mid - (result < 0 ? 1 : 2);
    }

    /**
     * Gets the position of the actual index summary entry in our Memory attribute, 'bytes'.
     * @param index The index of the entry or key to get the position for
     * @return an offset into our Memory attribute where the actual entry resides
     */
    public int getPositionInSummary(int index)
    {
        // The first section of bytes holds a four-byte position for each entry in the summary, so just multiply by 4.
        return offsets.getInt(index << 2);
    }

    public byte[] getKey(int index)
    {
        long start = getPositionInSummary(index);
        int keySize = (int) (calculateEnd(index) - start - 8L);
        byte[] key = new byte[keySize];
        entries.getBytes(start, key, 0, keySize);
        return key;
    }

    private void fillTemporaryKey(int index, ByteBuffer buffer)
    {
        long start = getPositionInSummary(index);
        int keySize = (int) (calculateEnd(index) - start - 8L);
        entries.setByteBuffer(buffer, start, keySize);
    }

    public long getPosition(int index)
    {
        return entries.getLong(calculateEnd(index) - 8);
    }

    public long getEndInSummary(int index)
    {
        return calculateEnd(index);
    }

    private long calculateEnd(int index)
    {
        return index == (offsetCount - 1) ? entriesLength : getPositionInSummary(index + 1);
    }

    public int getMinIndexInterval()
    {
        return minIndexInterval;
    }

    public double getEffectiveIndexInterval()
    {
        return (BASE_SAMPLING_LEVEL / (double) samplingLevel) * minIndexInterval;
    }

    /**
     * Returns an estimate of the total number of keys in the SSTable.
     */
    public long getEstimatedKeyCount()
    {
        return ((long) getMaxNumberOfEntries() + 1) * minIndexInterval;
    }

    public int size()
    {
        return offsetCount;
    }

    public int getSamplingLevel()
    {
        return samplingLevel;
    }

    /**
     * Returns the number of entries this summary would have if it were at the full sampling level, which is equal
     * to the number of entries in the primary on-disk index divided by the min index interval.
     */
    public int getMaxNumberOfEntries()
    {
        return sizeAtFullSampling;
    }

    /**
     * Returns the amount of off-heap memory used for the entries portion of this summary.
     * @return size in bytes
     */
    long getEntriesLength()
    {
        return entriesLength;
    }

    Memory getOffsets()
    {
        return offsets;
    }

    Memory getEntries()
    {
        return entries;
    }

    public long getOffHeapSize()
    {
        return offsetCount * 4 + entriesLength;
    }

    /**
     * Returns the number of primary (on-disk) index entries between the index summary entry at `index` and the next
     * index summary entry (assuming there is one).  Without any downsampling, this will always be equivalent to
     * the index interval.
     *
     * @param index the index of an index summary entry (between zero and the index entry size)
     *
     * @return the number of partitions after `index` until the next partition with a summary entry
     */
    public int getEffectiveIndexIntervalAfterIndex(int index)
    {
        return Downsampling.getEffectiveIndexIntervalAfterIndex(index, samplingLevel, minIndexInterval);
    }

    public IndexSummary sharedCopy()
    {
        return new IndexSummary(this);
    }

    public static class IndexSummarySerializer
    {
        public void serialize(IndexSummary t, DataOutputPlus out, boolean withSamplingLevel) throws IOException
        {
            out.writeInt(t.minIndexInterval);
            out.writeInt(t.offsetCount);
            out.writeLong(t.getOffHeapSize());
            if (withSamplingLevel)
            {
                out.writeInt(t.samplingLevel);
                out.writeInt(t.sizeAtFullSampling);
            }
            // our on-disk representation treats the offsets and the summary data as one contiguous structure,
            // in which the offsets are based from the start of the structure. i.e., if the offsets occupy
            // X bytes, the value of the first offset will be X. In memory we split the two regions up, so that
            // the summary values are indexed from zero, so we apply a correction to the offsets when de/serializing.
            // In this case adding X to each of the offsets.
            int baseOffset = t.offsetCount * 4;
            for (int i = 0 ; i < t.offsetCount ; i++)
            {
                int offset = t.offsets.getInt(i * 4) + baseOffset;
                // our serialization format for this file uses native byte order, so if this is different to the
                // default Java serialization order (BIG_ENDIAN) we have to reverse our bytes
                if (ByteOrder.nativeOrder() != ByteOrder.BIG_ENDIAN)
                    offset = Integer.reverseBytes(offset);
                out.writeInt(offset);
            }
            out.write(t.entries, 0, t.entriesLength);
        }

        public IndexSummary deserialize(DataInputStream in, IPartitioner partitioner, boolean haveSamplingLevel, int expectedMinIndexInterval, int maxIndexInterval) throws IOException
        {
            int minIndexInterval = in.readInt();
            if (minIndexInterval != expectedMinIndexInterval)
            {
                throw new IOException(String.format("Cannot read index summary because min_index_interval changed from %d to %d.",
                                                    minIndexInterval, expectedMinIndexInterval));
            }

            int offsetCount = in.readInt();
            long offheapSize = in.readLong();
            int samplingLevel, fullSamplingSummarySize;
            if (haveSamplingLevel)
            {
                samplingLevel = in.readInt();
                fullSamplingSummarySize = in.readInt();
            }
            else
            {
                samplingLevel = BASE_SAMPLING_LEVEL;
                fullSamplingSummarySize = offsetCount;
            }

            int effectiveIndexInterval = (int) Math.ceil((BASE_SAMPLING_LEVEL / (double) samplingLevel) * minIndexInterval);
            if (effectiveIndexInterval > maxIndexInterval)
            {
                throw new IOException(String.format("Rebuilding index summary because the effective index interval (%d) is higher than" +
                                                    " the current max index interval (%d)", effectiveIndexInterval, maxIndexInterval));
            }

            Memory offsets = Memory.allocate(offsetCount * 4);
            Memory entries = Memory.allocate(offheapSize - offsets.size());
            FBUtilities.copy(in, new MemoryOutputStream(offsets), offsets.size());
            FBUtilities.copy(in, new MemoryOutputStream(entries), entries.size());
            // our on-disk representation treats the offsets and the summary data as one contiguous structure,
            // in which the offsets are based from the start of the structure. i.e., if the offsets occupy
            // X bytes, the value of the first offset will be X. In memory we split the two regions up, so that
            // the summary values are indexed from zero, so we apply a correction to the offsets when de/serializing.
            // In this case subtracting X from each of the offsets.
            for (int i = 0 ; i < offsets.size() ; i += 4)
                offsets.setInt(i, (int) (offsets.getInt(i) - offsets.size()));
            return new IndexSummary(partitioner, offsets, offsetCount, entries, entries.size(), fullSamplingSummarySize, minIndexInterval, samplingLevel);
        }
    }
}
