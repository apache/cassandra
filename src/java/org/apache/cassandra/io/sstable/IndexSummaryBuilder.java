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

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemoryWriter;

import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;

public class IndexSummaryBuilder implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryBuilder.class);

    static final String defaultExpectedKeySizeName = Config.PROPERTY_PREFIX + "index_summary_expected_key_size";
    static long defaultExpectedKeySize = Long.valueOf(System.getProperty(defaultExpectedKeySizeName, "64"));

    // the offset in the keys memory region to look for a given summary boundary
    private final SafeMemoryWriter offsets;
    private final SafeMemoryWriter entries;

    private final int minIndexInterval;
    private final int samplingLevel;
    private final int[] startPoints;
    private long keysWritten = 0;
    private long indexIntervalMatches = 0;
    private long nextSamplePosition;

    // for each ReadableBoundary, we map its dataLength property to itself, permitting us to lookup the
    // last readable boundary from the perspective of the data file
    // [data file position limit] => [ReadableBoundary]
    private TreeMap<Long, ReadableBoundary> lastReadableByData = new TreeMap<>();
    // for each ReadableBoundary, we map its indexLength property to itself, permitting us to lookup the
    // last readable boundary from the perspective of the index file
    // [index file position limit] => [ReadableBoundary]
    private TreeMap<Long, ReadableBoundary> lastReadableByIndex = new TreeMap<>();
    // the last synced data file position
    private long dataSyncPosition;
    // the last synced index file position
    private long indexSyncPosition;

    // the last summary interval boundary that is fully readable in both data and index files
    private ReadableBoundary lastReadableBoundary;

    /**
     * Represents a boundary that is guaranteed fully readable in the summary, index file and data file.
     * The key contained is the last key readable if the index and data files have been flushed to the
     * stored lengths.
     */
    public static class ReadableBoundary
    {
        public final DecoratedKey lastKey;
        public final long indexLength;
        public final long dataLength;
        public final int summaryCount;
        public final long entriesLength;
        public ReadableBoundary(DecoratedKey lastKey, long indexLength, long dataLength, int summaryCount, long entriesLength)
        {
            this.lastKey = lastKey;
            this.indexLength = indexLength;
            this.dataLength = dataLength;
            this.summaryCount = summaryCount;
            this.entriesLength = entriesLength;
        }
    }

    /**
     * Build an index summary builder.
     *
     * @param expectedKeys - the number of keys we expect in the sstable
     * @param minIndexInterval - the minimum interval between entries selected for sampling
     * @param samplingLevel - the level at which entries are sampled
     */
    public IndexSummaryBuilder(long expectedKeys, int minIndexInterval, int samplingLevel)
    {
        this.samplingLevel = samplingLevel;
        this.startPoints = Downsampling.getStartPoints(BASE_SAMPLING_LEVEL, samplingLevel);

        long expectedEntrySize = getEntrySize(defaultExpectedKeySize);
        long maxExpectedEntries = expectedKeys / minIndexInterval;
        long maxExpectedEntriesSize = maxExpectedEntries * expectedEntrySize;
        if (maxExpectedEntriesSize > Integer.MAX_VALUE)
        {
            // that's a _lot_ of keys, and a very low min index interval
            int effectiveMinInterval = (int) Math.ceil((double)(expectedKeys * expectedEntrySize) / Integer.MAX_VALUE);
            maxExpectedEntries = expectedKeys / effectiveMinInterval;
            maxExpectedEntriesSize = maxExpectedEntries * expectedEntrySize;
            assert maxExpectedEntriesSize <= Integer.MAX_VALUE : maxExpectedEntriesSize;
            logger.warn("min_index_interval of {} is too low for {} expected keys of avg size {}; using interval of {} instead",
                        minIndexInterval, expectedKeys, defaultExpectedKeySize, effectiveMinInterval);
            this.minIndexInterval = effectiveMinInterval;
        }
        else
        {
            this.minIndexInterval = minIndexInterval;
        }

        // for initializing data structures, adjust our estimates based on the sampling level
        maxExpectedEntries = Math.max(1, (maxExpectedEntries * samplingLevel) / BASE_SAMPLING_LEVEL);
        offsets = new SafeMemoryWriter(4 * maxExpectedEntries).order(ByteOrder.nativeOrder());
        entries = new SafeMemoryWriter(expectedEntrySize * maxExpectedEntries).order(ByteOrder.nativeOrder());

        // the summary will always contain the first index entry (downsampling will never remove it)
        nextSamplePosition = 0;
        indexIntervalMatches++;
    }

    /**
     * Given a key, return how long the serialized index summary entry will be.
     */
    private static long getEntrySize(DecoratedKey key)
    {
        return getEntrySize(key.getKey().remaining());
    }

    /**
     * Given a key size, return how long the serialized index summary entry will be, that is add 8 bytes to
     * accomodate for the size of the position.
     */
    private static long getEntrySize(long keySize)
    {
        return keySize + TypeSizes.sizeof(0L);
    }

    // the index file has been flushed to the provided position; stash it and use that to recalculate our max readable boundary
    public void markIndexSynced(long upToPosition)
    {
        indexSyncPosition = upToPosition;
        refreshReadableBoundary();
    }

    // the data file has been flushed to the provided position; stash it and use that to recalculate our max readable boundary
    public void markDataSynced(long upToPosition)
    {
        dataSyncPosition = upToPosition;
        refreshReadableBoundary();
    }

    private void refreshReadableBoundary()
    {
        // grab the readable boundary prior to the given position in either the data or index file
        Map.Entry<?, ReadableBoundary> byData = lastReadableByData.floorEntry(dataSyncPosition);
        Map.Entry<?, ReadableBoundary> byIndex = lastReadableByIndex.floorEntry(indexSyncPosition);
        if (byData == null || byIndex == null)
            return;

        // take the lowest of the two, and stash it
        lastReadableBoundary = byIndex.getValue().indexLength < byData.getValue().indexLength
                               ? byIndex.getValue() : byData.getValue();

        // clear our data prior to this, since we no longer need it
        lastReadableByData.headMap(lastReadableBoundary.dataLength, false).clear();
        lastReadableByIndex.headMap(lastReadableBoundary.indexLength, false).clear();
    }

    public ReadableBoundary getLastReadableBoundary()
    {
        return lastReadableBoundary;
    }

    public IndexSummaryBuilder maybeAddEntry(DecoratedKey decoratedKey, long indexStart) throws IOException
    {
        return maybeAddEntry(decoratedKey, indexStart, 0, 0);
    }

    /**
     *
     * @param decoratedKey the key for this record
     * @param indexStart the position in the index file this record begins
     * @param indexEnd the position in the index file we need to be able to read to (exclusive) to read this record
     * @param dataEnd the position in the data file we need to be able to read to (exclusive) to read this record
     *                a value of 0 indicates we are not tracking readable boundaries
     */
    public IndexSummaryBuilder maybeAddEntry(DecoratedKey decoratedKey, long indexStart, long indexEnd, long dataEnd) throws IOException
    {
        if (keysWritten == nextSamplePosition)
        {
            if ((entries.length() + getEntrySize(decoratedKey)) <= Integer.MAX_VALUE)
            {
                offsets.writeInt((int) entries.length());
                entries.write(decoratedKey.getKey());
                entries.writeLong(indexStart);
                setNextSamplePosition(keysWritten);
            }
            else
            {
                // we cannot fully sample this sstable due to too much memory in the index summary, so let's tell the user
                logger.error("Memory capacity of index summary exceeded (2GB), index summary will not cover full sstable, " +
                             "you should increase min_sampling_level");
            }
        }
        else if (dataEnd != 0 && keysWritten + 1 == nextSamplePosition)
        {
            // this is the last key in this summary interval, so stash it
            ReadableBoundary boundary = new ReadableBoundary(decoratedKey, indexEnd, dataEnd, (int) (offsets.length() / 4), entries.length());
            lastReadableByData.put(dataEnd, boundary);
            lastReadableByIndex.put(indexEnd, boundary);
        }

        keysWritten++;
        return this;
    }

    // calculate the next key we will store to our summary
    private void setNextSamplePosition(long position)
    {
        tryAgain: while (true)
        {
            position += minIndexInterval;
            long test = indexIntervalMatches++;
            for (int start : startPoints)
                if ((test - start) % BASE_SAMPLING_LEVEL == 0)
                    continue tryAgain;

            nextSamplePosition = position;
            return;
        }
    }

    public void prepareToCommit()
    {
        // this method should only be called when we've finished appending records, so we truncate the
        // memory we're using to the exact amount required to represent it before building our summary
        entries.trim();
        offsets.trim();
    }

    public IndexSummary build(IPartitioner partitioner)
    {
        return build(partitioner, null);
    }

    // build the summary up to the provided boundary; this is backed by shared memory between
    // multiple invocations of this build method
    public IndexSummary build(IPartitioner partitioner, ReadableBoundary boundary)
    {
        assert entries.length() > 0;

        int count = (int) (offsets.length() / 4);
        long entriesLength = entries.length();
        if (boundary != null)
        {
            count = boundary.summaryCount;
            entriesLength = boundary.entriesLength;
        }

        int sizeAtFullSampling = (int) Math.ceil(keysWritten / (double) minIndexInterval);
        assert count > 0;
        return new IndexSummary(partitioner, offsets.currentBuffer().sharedCopy(),
                                count, entries.currentBuffer().sharedCopy(), entriesLength,
                                sizeAtFullSampling, minIndexInterval, samplingLevel);
    }

    // close the builder and release any associated memory
    public void close()
    {
        entries.close();
        offsets.close();
    }

    public Throwable close(Throwable accumulate)
    {
        accumulate = entries.close(accumulate);
        accumulate = offsets.close(accumulate);
        return accumulate;
    }

    static int entriesAtSamplingLevel(int samplingLevel, int maxSummarySize)
    {
        return (int) Math.ceil((samplingLevel * maxSummarySize) / (double) BASE_SAMPLING_LEVEL);
    }

    static int calculateSamplingLevel(int currentSamplingLevel, int currentNumEntries, long targetNumEntries, int minIndexInterval, int maxIndexInterval)
    {
        // effective index interval == (BASE_SAMPLING_LEVEL / samplingLevel) * minIndexInterval
        // so we can just solve for minSamplingLevel here:
        // maxIndexInterval == (BASE_SAMPLING_LEVEL / minSamplingLevel) * minIndexInterval
        int effectiveMinSamplingLevel = Math.max(1, (int) Math.ceil((BASE_SAMPLING_LEVEL * minIndexInterval) / (double) maxIndexInterval));

        // Algebraic explanation for calculating the new sampling level (solve for newSamplingLevel):
        // originalNumEntries = (baseSamplingLevel / currentSamplingLevel) * currentNumEntries
        // newSpaceUsed = (newSamplingLevel / baseSamplingLevel) * originalNumEntries
        // newSpaceUsed = (newSamplingLevel / baseSamplingLevel) * (baseSamplingLevel / currentSamplingLevel) * currentNumEntries
        // newSpaceUsed = (newSamplingLevel / currentSamplingLevel) * currentNumEntries
        // (newSpaceUsed * currentSamplingLevel) / currentNumEntries = newSamplingLevel
        int newSamplingLevel = (int) (targetNumEntries * currentSamplingLevel) / currentNumEntries;
        return Math.min(BASE_SAMPLING_LEVEL, Math.max(effectiveMinSamplingLevel, newSamplingLevel));
    }

    /**
     * Downsamples an existing index summary to a new sampling level.
     * @param existing an existing IndexSummary
     * @param newSamplingLevel the target level for the new IndexSummary.  This must be less than the current sampling
     *                         level for `existing`.
     * @param partitioner the partitioner used for the index summary
     * @return a new IndexSummary
     */
    @SuppressWarnings("resource")
    public static IndexSummary downsample(IndexSummary existing, int newSamplingLevel, int minIndexInterval, IPartitioner partitioner)
    {
        // To downsample the old index summary, we'll go through (potentially) several rounds of downsampling.
        // Conceptually, each round starts at position X and then removes every Nth item.  The value of X follows
        // a particular pattern to evenly space out the items that we remove.  The value of N decreases by one each
        // round.

        int currentSamplingLevel = existing.getSamplingLevel();
        assert currentSamplingLevel > newSamplingLevel;
        assert minIndexInterval == existing.getMinIndexInterval();

        // calculate starting indexes for downsampling rounds
        int[] startPoints = Downsampling.getStartPoints(currentSamplingLevel, newSamplingLevel);

        // calculate new off-heap size
        int newKeyCount = existing.size();
        long newEntriesLength = existing.getEntriesLength();
        for (int start : startPoints)
        {
            for (int j = start; j < existing.size(); j += currentSamplingLevel)
            {
                newKeyCount--;
                long length = existing.getEndInSummary(j) - existing.getPositionInSummary(j);
                newEntriesLength -= length;
            }
        }

        Memory oldEntries = existing.getEntries();
        Memory newOffsets = Memory.allocate(newKeyCount * 4);
        Memory newEntries = Memory.allocate(newEntriesLength);

        // Copy old entries to our new Memory.
        int i = 0;
        int newEntriesOffset = 0;
        outer:
        for (int oldSummaryIndex = 0; oldSummaryIndex < existing.size(); oldSummaryIndex++)
        {
            // to determine if we can skip this entry, go through the starting points for our downsampling rounds
            // and see if the entry's index is covered by that round
            for (int start : startPoints)
            {
                if ((oldSummaryIndex - start) % currentSamplingLevel == 0)
                    continue outer;
            }

            // write the position of the actual entry in the index summary (4 bytes)
            newOffsets.setInt(i * 4, newEntriesOffset);
            i++;
            long start = existing.getPositionInSummary(oldSummaryIndex);
            long length = existing.getEndInSummary(oldSummaryIndex) - start;
            newEntries.put(newEntriesOffset, oldEntries, start, length);
            newEntriesOffset += length;
        }
        assert newEntriesOffset == newEntriesLength;
        return new IndexSummary(partitioner, newOffsets, newKeyCount, newEntries, newEntriesLength,
                                existing.getMaxNumberOfEntries(), minIndexInterval, newSamplingLevel);
    }
}
