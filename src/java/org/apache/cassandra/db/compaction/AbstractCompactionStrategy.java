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
package org.apache.cassandra.db.compaction;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.StorageService;

/**
 * Pluggable compaction strategy determines how SSTables get merged.
 *
 * There are two main goals:
 *  - perform background compaction constantly as needed; this typically makes a tradeoff between
 *    i/o done by compaction, and merging done at read time.
 *  - perform a full (maximum possible) compaction if requested by the user
 */
public abstract class AbstractCompactionStrategy
{
    protected static final float DEFAULT_TOMBSTONE_THRESHOLD = 0.2f;
    protected static final String TOMBSTONE_THRESHOLD_KEY = "tombstone_threshold";

    protected final ColumnFamilyStore cfs;
    protected final Map<String, String> options;

    protected float tombstoneThreshold;

    protected AbstractCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.options = options;

        String optionValue = options.get(TOMBSTONE_THRESHOLD_KEY);
        tombstoneThreshold = (null != optionValue) ? Float.parseFloat(optionValue) : DEFAULT_TOMBSTONE_THRESHOLD;

        // start compactions in five minutes (if no flushes have occurred by then to do so)
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (CompactionManager.instance.getActiveCompactions() == 0)
                {
                    CompactionManager.instance.submitBackground(AbstractCompactionStrategy.this.cfs);
                }
            }
        };
        StorageService.optionalTasks.schedule(runnable, 5 * 60, TimeUnit.SECONDS);
    }

    public Map<String, String> getOptions()
    {
        return options;
    }

    /**
     * Releases any resources if this strategy is shutdown (when the CFS is reloaded after a schema change).
     * Default is to do nothing.
     */
    public void shutdown() { }

    /**
     * @return the next background/minor compaction task to run; null if nothing to do.
     * @param gcBefore throw away tombstones older than this
     */
    public abstract AbstractCompactionTask getNextBackgroundTask(final int gcBefore);

    /**
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     * @param gcBefore throw away tombstones older than this
     */
    public abstract AbstractCompactionTask getMaximalTask(final int gcBefore);

    /**
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     * @param gcBefore throw away tombstones older than this
     */
    public abstract AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore);

    /**
     * @return the number of background tasks estimated to still be needed for this columnfamilystore
     */
    public abstract int getEstimatedRemainingTasks();

    /**
     * @return size in bytes of the largest sstables for this strategy
     */
    public abstract long getMaxSSTableSize();

    /**
     * @return true if checking for whether a key exists, ignoring @param sstablesToIgnore,
     * is going to be expensive
     */
    public abstract boolean isKeyExistenceExpensive(Set<? extends SSTable> sstablesToIgnore);

    /**
     * Filters SSTables that are to be blacklisted from the given collection
     *
     * @param originalCandidates The collection to check for blacklisted SSTables
     *
     * @return list of the SSTables with blacklisted ones filtered out
     */
    public static List<SSTableReader> filterSuspectSSTables(Collection<SSTableReader> originalCandidates)
    {
        List<SSTableReader> filteredCandidates = new ArrayList<SSTableReader>();

        for (SSTableReader candidate : originalCandidates)
        {
            if (!candidate.isMarkedSuspect())
                filteredCandidates.add(candidate);
        }

        return filteredCandidates;
    }

    /**
     * Returns a list of KeyScanners given sstables and a range on which to scan.
     * The default implementation simply grab one SSTableScanner per-sstable, but overriding this method
     * allow for a more memory efficient solution if we know the sstable don't overlap (see
     * LeveledCompactionStrategy for instance).
     */
    public List<ICompactionScanner> getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        ArrayList<ICompactionScanner> scanners = new ArrayList<ICompactionScanner>();
        for (SSTableReader sstable : sstables)
            scanners.add(sstable.getDirectScanner(range));
        return scanners;
    }

    public List<ICompactionScanner> getScanners(Collection<SSTableReader> toCompact)
    {
        return getScanners(toCompact, null);
    }

    /**
     * @param sstable SSTable to check
     * @param gcBefore time to drop tombstones
     * @return true if given sstable's tombstones are expected to be removed
     */
    protected boolean worthDroppingTombstones(SSTableReader sstable, int gcBefore)
    {
        double droppableRatio = sstable.getEstimatedDroppableTombstoneRatio(gcBefore);
        if (droppableRatio <= tombstoneThreshold)
            return false;

        Set<SSTableReader> overlaps = cfs.getOverlappingSSTables(Collections.singleton(sstable));
        if (overlaps.isEmpty())
        {
            // there is no overlap, tombstones are safely droppable
            return true;
        }
        else
        {
            // what percentage of columns do we expect to compact outside of overlap?
            // first, calculate estimated keys that do not overlap
            long keys = sstable.estimatedKeys();
            Set<Range<Token>> ranges = new HashSet<Range<Token>>();
            for (SSTableReader overlap : overlaps)
                ranges.add(new Range<Token>(overlap.first.token, overlap.last.token, overlap.partitioner));
            long remainingKeys = keys - sstable.estimatedKeysForRanges(ranges);
            // next, calculate what percentage of columns we have within those keys
            double remainingKeysRatio = ((double) remainingKeys) / keys;
            long columns = sstable.getEstimatedColumnCount().percentile(remainingKeysRatio) * remainingKeys;
            double remainingColumnsRatio = ((double) columns) / (sstable.getEstimatedColumnCount().count() * sstable.getEstimatedColumnCount().mean());

            // return if we still expect to have droppable tombstones in rest of columns
            return remainingColumnsRatio * droppableRatio > tombstoneThreshold;
        }
    }
}
