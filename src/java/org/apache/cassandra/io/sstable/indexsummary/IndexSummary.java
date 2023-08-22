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
package org.apache.cassandra.io.sstable.indexsummary;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;
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
    private static final Logger logger = LoggerFactory.getLogger(IndexSummary.class);
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
        assert samplingLevel > 0;
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
    public int binarySearch(PartitionPosition key)
    {
        // We will be comparing non-native Keys, so use a buffer with appropriate byte order
        ByteBuffer hollow = MemoryUtil.getHollowDirectByteBuffer().order(ByteOrder.BIG_ENDIAN);
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

    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        identities.add(offsets);
        identities.add(entries);
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

    public List<SSTableReader.IndexesBounds> getSampleIndexesForRanges(Collection<Range<Token>> ranges)
    {
        // use the index to determine a minimal section for each range
        List<SSTableReader.IndexesBounds> positions = new ArrayList<>();

        for (Range<Token> range : Range.normalize(ranges))
        {
            PartitionPosition leftPosition = range.left.maxKeyBound();
            PartitionPosition rightPosition = range.right.maxKeyBound();

            int left = binarySearch(leftPosition);
            if (left < 0)
                left = (left + 1) * -1;
            else
                // left range are start exclusive
                left = left + 1;
            if (left == size())
                // left is past the end of the sampling
                continue;

            int right = Range.isWrapAround(range.left, range.right)
                        ? size() - 1
                        : binarySearch(rightPosition);
            if (right < 0)
            {
                // range are end inclusive so we use the previous index from what binarySearch give us
                // since that will be the last index we will return
                right = (right + 1) * -1;
                if (right == 0)
                    // Means the first key is already stricly greater that the right bound
                    continue;
                right--;
            }

            if (left > right)
                // empty range
                continue;
            positions.add(new SSTableReader.IndexesBounds(left, right));
        }
        return positions;
    }

    public Iterable<byte[]> getKeySamples(final Range<Token> range)
    {
        final List<SSTableReader.IndexesBounds> indexRanges = getSampleIndexesForRanges(Collections.singletonList(range));

        if (indexRanges.isEmpty())
            return Collections.emptyList();

        return () -> new Iterator<byte[]>()
        {
            private Iterator<SSTableReader.IndexesBounds> rangeIter = indexRanges.iterator();
            private SSTableReader.IndexesBounds current;
            private int idx;

            public boolean hasNext()
            {
                if (current == null || idx > current.upperPosition)
                {
                    if (rangeIter.hasNext())
                    {
                        current = rangeIter.next();
                        idx = current.lowerPosition;
                        return true;
                    }
                    return false;
                }

                return true;
            }

            public byte[] next()
            {
                return getKey(idx++);
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public long getScanPosition(PartitionPosition key)
    {
        return getScanPositionFromBinarySearchResult(binarySearch(key));
    }

    @VisibleForTesting
    public long getScanPositionFromBinarySearchResult(int binarySearchResult)
    {
        if (binarySearchResult == -1)
            return 0;
        else
            return getPosition(getIndexFromBinarySearchResult(binarySearchResult));
    }

    public static int getIndexFromBinarySearchResult(int binarySearchResult)
    {
        if (binarySearchResult < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (binarySearchResult + 1) * -1;
            if (greaterThan == 0)
                return -1;
            return greaterThan - 1;
        }
        else
        {
            return binarySearchResult;
        }
    }

    public IndexSummary sharedCopy()
    {
        return new IndexSummary(this);
    }

    public static class IndexSummarySerializer
    {
        public void serialize(IndexSummary t, DataOutputPlus out) throws IOException
        {
            out.writeInt(t.minIndexInterval);
            out.writeInt(t.offsetCount);
            out.writeLong(t.getOffHeapSize());
            out.writeInt(t.samplingLevel);
            out.writeInt(t.sizeAtFullSampling);
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
                offset = Integer.reverseBytes(offset);
                out.writeInt(offset);
            }
            out.write(t.entries, 0, t.entriesLength);
        }

        public <T extends InputStream & DataInputPlus> IndexSummary deserialize(T in, IPartitioner partitioner, int expectedMinIndexInterval, int maxIndexInterval) throws IOException
        {
            int minIndexInterval = in.readInt();
            if (minIndexInterval != expectedMinIndexInterval)
            {
                throw new IOException(String.format("Cannot read index summary because min_index_interval changed from %d to %d.",
                                                    minIndexInterval, expectedMinIndexInterval));
            }

            int offsetCount = in.readInt();
            long offheapSize = in.readLong();
            int samplingLevel = in.readInt();
            int fullSamplingSummarySize = in.readInt();

            int effectiveIndexInterval = (int) Math.ceil((BASE_SAMPLING_LEVEL / (double) samplingLevel) * minIndexInterval);
            if (effectiveIndexInterval > maxIndexInterval)
            {
                throw new IOException(String.format("Rebuilding index summary because the effective index interval (%d) is higher than" +
                                                    " the current max index interval (%d)", effectiveIndexInterval, maxIndexInterval));
            }

            Memory offsets = Memory.allocate(offsetCount * 4);
            Memory entries = Memory.allocate(offheapSize - offsets.size());
            try
            {
                FBUtilities.copy(in, new MemoryOutputStream(offsets), offsets.size());
                FBUtilities.copy(in, new MemoryOutputStream(entries), entries.size());
            }
            catch (IOException ioe)
            {
                offsets.free();
                entries.free();
                throw ioe;
            }
            // our on-disk representation treats the offsets and the summary data as one contiguous structure,
            // in which the offsets are based from the start of the structure. i.e., if the offsets occupy
            // X bytes, the value of the first offset will be X. In memory we split the two regions up, so that
            // the summary values are indexed from zero, so we apply a correction to the offsets when de/serializing.
            // In this case subtracting X from each of the offsets.
            for (int i = 0 ; i < offsets.size() ; i += 4)
                offsets.setInt(i, (int) (offsets.getInt(i) - offsets.size()));
            return new IndexSummary(partitioner, offsets, offsetCount, entries, entries.size(), fullSamplingSummarySize, minIndexInterval, samplingLevel);
        }

        /**
         * Deserializes the first and last key stored in the summary
         * <p>
         * Only for use by offline tools like SSTableMetadataViewer, otherwise SSTable.first/last should be used.
         */
        public Pair<DecoratedKey, DecoratedKey> deserializeFirstLastKey(DataInputStreamPlus in, IPartitioner partitioner) throws IOException
        {
            in.skipBytes(4); // minIndexInterval
            int offsetCount = in.readInt();
            long offheapSize = in.readLong();
            in.skipBytes(8); // samplingLevel, fullSamplingSummarySize

            in.skipBytes(offsetCount * 4);
            in.skipBytes((int) (offheapSize - offsetCount * 4));

            DecoratedKey first = partitioner.decorateKey(ByteBufferUtil.readWithLength(in));
            DecoratedKey last = partitioner.decorateKey(ByteBufferUtil.readWithLength(in));
            return Pair.create(first, last);
        }
    }
}
