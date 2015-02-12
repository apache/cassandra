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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.RefCountedMemory;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.Memory;

import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;
import static org.apache.cassandra.io.sstable.SSTable.getMinimalKey;

public class IndexSummaryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryBuilder.class);

    private final ArrayList<Long> positions;
    private final ArrayList<DecoratedKey> keys;
    private final int minIndexInterval;
    private final int samplingLevel;
    private final int[] startPoints;
    private long keysWritten = 0;
    private long indexIntervalMatches = 0;
    private long offheapSize = 0;
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
        public ReadableBoundary(DecoratedKey lastKey, long indexLength, long dataLength)
        {
            this.lastKey = lastKey;
            this.indexLength = indexLength;
            this.dataLength = dataLength;
        }
    }

    public IndexSummaryBuilder(long expectedKeys, int minIndexInterval, int samplingLevel)
    {
        this.samplingLevel = samplingLevel;
        this.startPoints = Downsampling.getStartPoints(BASE_SAMPLING_LEVEL, samplingLevel);

        long maxExpectedEntries = expectedKeys / minIndexInterval;
        if (maxExpectedEntries > Integer.MAX_VALUE)
        {
            // that's a _lot_ of keys, and a very low min index interval
            int effectiveMinInterval = (int) Math.ceil((double) Integer.MAX_VALUE / expectedKeys);
            maxExpectedEntries = expectedKeys / effectiveMinInterval;
            assert maxExpectedEntries <= Integer.MAX_VALUE : maxExpectedEntries;
            logger.warn("min_index_interval of {} is too low for {} expected keys; using interval of {} instead",
                        minIndexInterval, expectedKeys, effectiveMinInterval);
            this.minIndexInterval = effectiveMinInterval;
        }
        else
        {
            this.minIndexInterval = minIndexInterval;
        }

        // for initializing data structures, adjust our estimates based on the sampling level
        maxExpectedEntries = (maxExpectedEntries * samplingLevel) / BASE_SAMPLING_LEVEL;
        positions = new ArrayList<>((int)maxExpectedEntries);
        keys = new ArrayList<>((int)maxExpectedEntries);
        // if we're downsampling we may not use index 0
        setNextSamplePosition(-minIndexInterval);
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

    public IndexSummaryBuilder maybeAddEntry(DecoratedKey decoratedKey, long indexStart)
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
    public IndexSummaryBuilder maybeAddEntry(DecoratedKey decoratedKey, long indexStart, long indexEnd, long dataEnd)
    {
        if (keysWritten == nextSamplePosition)
        {
            keys.add(getMinimalKey(decoratedKey));
            offheapSize += decoratedKey.getKey().remaining();
            positions.add(indexStart);
            offheapSize += TypeSizes.NATIVE.sizeof(indexStart);
            setNextSamplePosition(keysWritten);
        }
        else if (dataEnd != 0 && keysWritten + 1 == nextSamplePosition)
        {
            // this is the last key in this summary interval, so stash it
            ReadableBoundary boundary = new ReadableBoundary(decoratedKey, indexEnd, dataEnd);
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

    public IndexSummary build(IPartitioner partitioner)
    {
        return build(partitioner, null);
    }

    // lastIntervalKey should come from getLastReadableBoundary().lastKey
    public IndexSummary build(IPartitioner partitioner, DecoratedKey lastIntervalKey)
    {
        assert keys.size() > 0;
        assert keys.size() == positions.size();

        int length;
        if (lastIntervalKey == null)
            length = keys.size();
        else // since it's an inclusive upper bound, this should never match exactly
            length = -1 -Collections.binarySearch(keys, lastIntervalKey);

        assert length > 0;

        long offheapSize = this.offheapSize;
        if (length < keys.size())
            for (int i = length ; i < keys.size() ; i++)
                offheapSize -= keys.get(i).getKey().remaining() + TypeSizes.NATIVE.sizeof(positions.get(i));

        // first we write out the position in the *summary* for each key in the summary,
        // then we write out (key, actual index position) pairs
        Memory memory = Memory.allocate(offheapSize + (length * 4));
        int idxPosition = 0;
        int keyPosition = length * 4;
        for (int i = 0; i < length; i++)
        {
            // write the position of the actual entry in the index summary (4 bytes)
            memory.setInt(idxPosition, keyPosition);
            idxPosition += TypeSizes.NATIVE.sizeof(keyPosition);

            // write the key
            ByteBuffer keyBytes = keys.get(i).getKey();
            memory.setBytes(keyPosition, keyBytes);
            keyPosition += keyBytes.remaining();

            // write the position in the actual index file
            long actualIndexPosition = positions.get(i);
            memory.setLong(keyPosition, actualIndexPosition);
            keyPosition += TypeSizes.NATIVE.sizeof(actualIndexPosition);
        }
        assert keyPosition == offheapSize + (length * 4);
        int sizeAtFullSampling = (int) Math.ceil(keysWritten / (double) minIndexInterval);
        return new IndexSummary(partitioner, memory, length, sizeAtFullSampling, minIndexInterval, samplingLevel);
    }

    public static int entriesAtSamplingLevel(int samplingLevel, int maxSummarySize)
    {
        return (int) Math.ceil((samplingLevel * maxSummarySize) / (double) BASE_SAMPLING_LEVEL);
    }

    public static int calculateSamplingLevel(int currentSamplingLevel, int currentNumEntries, long targetNumEntries, int minIndexInterval, int maxIndexInterval)
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
        int removedKeyCount = 0;
        long newOffHeapSize = existing.getOffHeapSize();
        for (int start : startPoints)
        {
            for (int j = start; j < existing.size(); j += currentSamplingLevel)
            {
                removedKeyCount++;
                newOffHeapSize -= existing.getEntry(j).length;
            }
        }

        int newKeyCount = existing.size() - removedKeyCount;

        // Subtract (removedKeyCount * 4) from the new size to account for fewer entries in the first section, which
        // stores the position of the actual entries in the summary.
        RefCountedMemory memory = new RefCountedMemory(newOffHeapSize - (removedKeyCount * 4));

        // Copy old entries to our new Memory.
        int idxPosition = 0;
        int keyPosition = newKeyCount * 4;
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
            memory.setInt(idxPosition, keyPosition);
            idxPosition += TypeSizes.NATIVE.sizeof(keyPosition);

            // write the entry itself
            byte[] entry = existing.getEntry(oldSummaryIndex);
            memory.setBytes(keyPosition, entry, 0, entry.length);
            keyPosition += entry.length;
        }
        return new IndexSummary(partitioner, memory, newKeyCount, existing.getMaxNumberOfEntries(),
                                minIndexInterval, newSamplingLevel);
    }
}
