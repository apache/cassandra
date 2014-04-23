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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.RefCountedMemory;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.FBUtilities;

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
public class IndexSummary implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummary.class);

    public static final IndexSummarySerializer serializer = new IndexSummarySerializer();

    /**
     * A lower bound for the average number of partitions in between each index summary entry. A lower value means
     * that more partitions will have an entry in the index summary when at the full sampling level.
     */
    private final int minIndexInterval;

    private final IPartitioner partitioner;
    private final int summarySize;
    private final int sizeAtFullSampling;
    private final RefCountedMemory bytes;

    /**
     * A value between 1 and BASE_SAMPLING_LEVEL that represents how many of the original
     * index summary entries ((1 / indexInterval) * numKeys) have been retained.
     *
     * Thus, this summary contains (samplingLevel / BASE_SAMPLING_LEVEL) * ((1 / indexInterval) * numKeys)) entries.
     */
    private final int samplingLevel;

    public IndexSummary(IPartitioner partitioner, RefCountedMemory memory, int summarySize, int sizeAtFullSampling,
                        int minIndexInterval, int samplingLevel)
    {
        this.partitioner = partitioner;
        this.minIndexInterval = minIndexInterval;
        this.summarySize = summarySize;
        this.sizeAtFullSampling = sizeAtFullSampling;
        this.bytes = memory;
        this.samplingLevel = samplingLevel;
    }

    // binary search is notoriously more difficult to get right than it looks; this is lifted from
    // Harmony's Collections implementation
    public int binarySearch(RowPosition key)
    {
        int low = 0, mid = summarySize, high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            result = -DecoratedKey.compareTo(partitioner, ByteBuffer.wrap(getKey(mid)), key);
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
        return bytes.getInt(index << 2);
    }

    public byte[] getKey(int index)
    {
        long start = getPositionInSummary(index);
        int keySize = (int) (calculateEnd(index) - start - 8L);
        byte[] key = new byte[keySize];
        bytes.getBytes(start, key, 0, keySize);
        return key;
    }

    public long getPosition(int index)
    {
        return bytes.getLong(calculateEnd(index) - 8);
    }

    public byte[] getEntry(int index)
    {
        long start = getPositionInSummary(index);
        long end = calculateEnd(index);
        byte[] entry = new byte[(int)(end - start)];
        bytes.getBytes(start, entry, 0, (int)(end - start));
        return entry;
    }

    private long calculateEnd(int index)
    {
        return index == (summarySize - 1) ? bytes.size() : getPositionInSummary(index + 1);
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
        return summarySize;
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
     * Returns the amount of off-heap memory used for this summary.
     * @return size in bytes
     */
    public long getOffHeapSize()
    {
        return bytes.size();
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

    public static class IndexSummarySerializer
    {
        public void serialize(IndexSummary t, DataOutputPlus out, boolean withSamplingLevel) throws IOException
        {
            out.writeInt(t.minIndexInterval);
            out.writeInt(t.summarySize);
            out.writeLong(t.bytes.size());
            if (withSamplingLevel)
            {
                out.writeInt(t.samplingLevel);
                out.writeInt(t.sizeAtFullSampling);
            }
            out.write(t.bytes);
        }

        public IndexSummary deserialize(DataInputStream in, IPartitioner partitioner, boolean haveSamplingLevel, int expectedMinIndexInterval, int maxIndexInterval) throws IOException
        {
            int minIndexInterval = in.readInt();
            if (minIndexInterval != expectedMinIndexInterval)
            {
                throw new IOException(String.format("Cannot read index summary because min_index_interval changed from %d to %d.",
                                                    minIndexInterval, expectedMinIndexInterval));
            }

            int summarySize = in.readInt();
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
                fullSamplingSummarySize = summarySize;
            }

            int effectiveIndexInterval = (int) Math.ceil((BASE_SAMPLING_LEVEL / (double) samplingLevel) * minIndexInterval);
            if (effectiveIndexInterval > maxIndexInterval)
            {
                throw new IOException(String.format("Rebuilding index summary because the effective index interval (%d) is higher than" +
                                                    " the current max index interval (%d)", effectiveIndexInterval, maxIndexInterval));
            }

            RefCountedMemory memory = new RefCountedMemory(offheapSize);
            FBUtilities.copy(in, new MemoryOutputStream(memory), offheapSize);
            return new IndexSummary(partitioner, memory, summarySize, fullSamplingSummarySize, minIndexInterval, samplingLevel);
        }
    }

    @Override
    public void close()
    {
        bytes.unreference();
    }

    public IndexSummary readOnlyClone()
    {
        bytes.reference();
        return this;
    }
}
