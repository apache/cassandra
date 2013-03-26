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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTableReader;

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
    private static final Logger logger = LoggerFactory.getLogger(AbstractCompactionStrategy.class);

    protected static final float DEFAULT_TOMBSTONE_THRESHOLD = 0.2f;
    // minimum interval needed to perform tombstone removal compaction in seconds, default 86400 or 1 day.
    protected static final long DEFAULT_TOMBSTONE_COMPACTION_INTERVAL = 86400;
    protected static final String TOMBSTONE_THRESHOLD_OPTION = "tombstone_threshold";
    protected static final String TOMBSTONE_COMPACTION_INTERVAL_OPTION = "tombstone_compaction_interval";

    public final Map<String, String> options;

    protected final ColumnFamilyStore cfs;
    protected float tombstoneThreshold;
    protected long tombstoneCompactionInterval;

    protected AbstractCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.options = options;

        /* checks must be repeated here, as user supplied strategies might not call validateOptions directly */

        try
        {
            validateOptions(options);
            String optionValue = options.get(TOMBSTONE_THRESHOLD_OPTION);
            tombstoneThreshold = optionValue == null ? DEFAULT_TOMBSTONE_THRESHOLD : Float.parseFloat(optionValue);
            optionValue = options.get(TOMBSTONE_COMPACTION_INTERVAL_OPTION);
            tombstoneCompactionInterval = optionValue == null ? DEFAULT_TOMBSTONE_COMPACTION_INTERVAL : Long.parseLong(optionValue);
        }
        catch (ConfigurationException e)
        {
            logger.warn("Error setting compaction strategy options ({}), defaults will be used", e.getMessage());
            tombstoneThreshold = DEFAULT_TOMBSTONE_THRESHOLD;
            tombstoneCompactionInterval = DEFAULT_TOMBSTONE_COMPACTION_INTERVAL;
        }
    }

    /**
     * Releases any resources if this strategy is shutdown (when the CFS is reloaded after a schema change).
     * Default is to do nothing.
     */
    public void shutdown() { }

    /**
     * @param gcBefore throw away tombstones older than this
     *
     * @return the next background/minor compaction task to run; null if nothing to do.
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    public abstract AbstractCompactionTask getNextBackgroundTask(final int gcBefore);

    /**
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    public abstract AbstractCompactionTask getMaximalTask(final int gcBefore);

    /**
     * @param sstables SSTables to compact. Must be marked as compacting.
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     *
     * Is responsible for marking its sstables as compaction-pending.
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
     * Filters SSTables that are to be blacklisted from the given collection
     *
     * @param originalCandidates The collection to check for blacklisted SSTables
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
     * Check if given sstable is worth dropping tombstones at gcBefore.
     * Check is skipped if tombstone_compaction_interval time does not elapse since sstable creation and returns false.
     *
     * @param sstable SSTable to check
     * @param gcBefore time to drop tombstones
     * @return true if given sstable's tombstones are expected to be removed
     */
    protected boolean worthDroppingTombstones(SSTableReader sstable, int gcBefore)
    {
        // since we use estimations to calculate, there is a chance that compaction will not drop tombstones actually.
        // if that happens we will end up in infinite compaction loop, so first we check enough if enough time has
        // elapsed since SSTable created.
        if (System.currentTimeMillis() < sstable.getCreationTimeFor(Component.DATA) + tombstoneCompactionInterval * 1000)
           return false;

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
            if (sstable.getKeySamples().size() < 2)
            {
                // we have too few samples to estimate correct percentage
                return false;
            }
            // first, calculate estimated keys that do not overlap
            long keys = sstable.estimatedKeys();
            Set<Range<Token>> ranges = new HashSet<Range<Token>>();
            for (SSTableReader overlap : overlaps)
                ranges.add(new Range<Token>(overlap.first.token, overlap.last.token, overlap.partitioner));
            long remainingKeys = keys - sstable.estimatedKeysForRanges(ranges);
            // next, calculate what percentage of columns we have within those keys
            long columns = sstable.getEstimatedColumnCount().mean() * remainingKeys;
            double remainingColumnsRatio = ((double) columns) / (sstable.getEstimatedColumnCount().count() * sstable.getEstimatedColumnCount().mean());

            // return if we still expect to have droppable tombstones in rest of columns
            return remainingColumnsRatio * droppableRatio > tombstoneThreshold;
        }
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        String threshold = options.get(TOMBSTONE_THRESHOLD_OPTION);
        if (threshold != null)
        {
            try
            {
                float thresholdValue = Float.parseFloat(threshold);
                if (thresholdValue < 0)
                {
                    throw new ConfigurationException(String.format("%s must be greater than 0, but was %f", TOMBSTONE_THRESHOLD_OPTION, thresholdValue));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", threshold, TOMBSTONE_THRESHOLD_OPTION), e);
            }
        }

        String interval = options.get(TOMBSTONE_COMPACTION_INTERVAL_OPTION);
        if (interval != null)
        {
            try
            {
                long tombstoneCompactionInterval = Long.parseLong(interval);
                if (tombstoneCompactionInterval < 0)
                {
                    throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", TOMBSTONE_COMPACTION_INTERVAL_OPTION, tombstoneCompactionInterval));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", interval, TOMBSTONE_COMPACTION_INTERVAL_OPTION), e);
            }
        }

        Map<String, String> uncheckedOptions = new HashMap<String, String>(options);
        uncheckedOptions.remove(TOMBSTONE_THRESHOLD_OPTION);
        uncheckedOptions.remove(TOMBSTONE_COMPACTION_INTERVAL_OPTION);
        return uncheckedOptions;
    }
}
