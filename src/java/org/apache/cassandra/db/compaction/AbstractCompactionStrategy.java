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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.utils.JVMStabilityInspector;

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
    protected static final boolean DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION_OPTION = false;

    protected static final String TOMBSTONE_THRESHOLD_OPTION = "tombstone_threshold";
    protected static final String TOMBSTONE_COMPACTION_INTERVAL_OPTION = "tombstone_compaction_interval";
    // disable range overlap check when deciding if an SSTable is candidate for tombstone compaction (CASSANDRA-6563)
    protected static final String UNCHECKED_TOMBSTONE_COMPACTION_OPTION = "unchecked_tombstone_compaction";
    protected static final String COMPACTION_ENABLED = "enabled";

    public final Map<String, String> options;

    protected final ColumnFamilyStore cfs;
    protected float tombstoneThreshold;
    protected long tombstoneCompactionInterval;
    protected boolean uncheckedTombstoneCompaction;

    /**
     * pause/resume/getNextBackgroundTask must synchronize.  This guarantees that after pause completes,
     * no new tasks will be generated; or put another way, pause can't run until in-progress tasks are
     * done being created.
     *
     * This allows runWithCompactionsDisabled to be confident that after pausing, once in-progress
     * tasks abort, it's safe to proceed with truncate/cleanup/etc.
     *
     * See CASSANDRA-3430
     */
    protected boolean isActive = false;

    protected volatile boolean enabled = true;

    protected AbstractCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.options = ImmutableMap.copyOf(options);

        /* checks must be repeated here, as user supplied strategies might not call validateOptions directly */

        try
        {
            validateOptions(options);
            String optionValue = options.get(TOMBSTONE_THRESHOLD_OPTION);
            tombstoneThreshold = optionValue == null ? DEFAULT_TOMBSTONE_THRESHOLD : Float.parseFloat(optionValue);
            optionValue = options.get(TOMBSTONE_COMPACTION_INTERVAL_OPTION);
            tombstoneCompactionInterval = optionValue == null ? DEFAULT_TOMBSTONE_COMPACTION_INTERVAL : Long.parseLong(optionValue);
            optionValue = options.get(UNCHECKED_TOMBSTONE_COMPACTION_OPTION);
            uncheckedTombstoneCompaction = optionValue == null ? DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION_OPTION : Boolean.parseBoolean(optionValue);
            if (!shouldBeEnabled())
                this.disable();
        }
        catch (ConfigurationException e)
        {
            logger.warn("Error setting compaction strategy options ({}), defaults will be used", e.getMessage());
            tombstoneThreshold = DEFAULT_TOMBSTONE_THRESHOLD;
            tombstoneCompactionInterval = DEFAULT_TOMBSTONE_COMPACTION_INTERVAL;
            uncheckedTombstoneCompaction = DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION_OPTION;
        }
    }

    /**
     * For internal, temporary suspension of background compactions so that we can do exceptional
     * things like truncate or major compaction
     */
    public synchronized void pause()
    {
        isActive = false;
    }

    /**
     * For internal, temporary suspension of background compactions so that we can do exceptional
     * things like truncate or major compaction
     */
    public synchronized void resume()
    {
        isActive = true;
    }

    /**
     * Performs any extra initialization required
     */
    public void startup()
    {
        isActive = true;
    }

    /**
     * Releases any resources if this strategy is shutdown (when the CFS is reloaded after a schema change).
     */
    public void shutdown()
    {
        isActive = false;
    }

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
    public abstract Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore);

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

    public AbstractCompactionTask getCompactionTask(Collection<SSTableReader> sstables, final int gcBefore, long maxSSTableBytes)
    {
        return new CompactionTask(cfs, sstables, gcBefore, false);
    }

    /**
     * @return the number of background tasks estimated to still be needed for this columnfamilystore
     */
    public abstract int getEstimatedRemainingTasks();

    /**
     * @return size in bytes of the largest sstables for this strategy
     */
    public abstract long getMaxSSTableBytes();

    public boolean isEnabled()
    {
        return this.enabled && this.isActive;
    }

    public void enable()
    {
        this.enabled = true;
    }

    public void disable()
    {
        this.enabled = false;
    }

    /**
     * @return whether or not MeteredFlusher should be able to trigger memtable flushes for this CF.
     */
    public boolean isAffectedByMeteredFlusher()
    {
        return true;
    }

    /**
     * If not affected by MeteredFlusher (and handling flushing on its own), override to tell MF how much
     * space to reserve for this CF, i.e., how much space to subtract from `memtable_total_space_in_mb` when deciding
     * if other memtables should be flushed or not.
     */
    public long getMemtableReservedSize()
    {
        return 0;
    }

    /**
     * Handle a flushed memtable.
     *
     * @param memtable the flushed memtable
     * @param sstable the written sstable. can be null if the memtable was clean.
     */
    public void replaceFlushed(Memtable memtable, SSTableReader sstable)
    {
        cfs.getDataTracker().replaceFlushed(memtable, sstable);
        if (sstable != null)
            CompactionManager.instance.submitBackground(cfs);
    }

    /**
     * @return a subset of the suggested sstables that are relevant for read requests.
     */
    public List<SSTableReader> filterSSTablesForReads(List<SSTableReader> sstables)
    {
        return sstables;
    }

    /**
     * Filters SSTables that are to be blacklisted from the given collection
     *
     * @param originalCandidates The collection to check for blacklisted SSTables
     * @return list of the SSTables with blacklisted ones filtered out
     */
    public static Iterable<SSTableReader> filterSuspectSSTables(Iterable<SSTableReader> originalCandidates)
    {
        return Iterables.filter(originalCandidates, new Predicate<SSTableReader>()
        {
            public boolean apply(SSTableReader sstable)
            {
                return !sstable.isMarkedSuspect();
            }
        });
    }

    /**
     * Returns a list of KeyScanners given sstables and a range on which to scan.
     * The default implementation simply grab one SSTableScanner per-sstable, but overriding this method
     * allow for a more memory efficient solution if we know the sstable don't overlap (see
     * LeveledCompactionStrategy for instance).
     */
    public ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        RateLimiter limiter = CompactionManager.instance.getRateLimiter();
        ArrayList<ISSTableScanner> scanners = new ArrayList<ISSTableScanner>();
        try
        {
            for (SSTableReader sstable : sstables)
                scanners.add(sstable.getScanner(range, limiter));
        }
        catch (Throwable t)
        {
            try
            {
                new ScannerList(scanners).close();
            }
            catch (Throwable t2)
            {
                t.addSuppressed(t2);
            }
            throw t;
        }
        return new ScannerList(scanners);
    }

    public boolean shouldDefragment()
    {
        return false;
    }

    public String getName()
    {
        return getClass().getSimpleName();
    }

    public synchronized void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        for (SSTableReader remove : removed)
            removeSSTable(remove);
        for (SSTableReader add : added)
            addSSTable(add);
    }

    public abstract void addSSTable(SSTableReader added);

    public abstract void removeSSTable(SSTableReader sstable);

    public static class ScannerList implements AutoCloseable
    {
        public final List<ISSTableScanner> scanners;
        public ScannerList(List<ISSTableScanner> scanners)
        {
            this.scanners = scanners;
        }

        public void close()
        {
            Throwable t = null;
            for (ISSTableScanner scanner : scanners)
            {
                try
                {
                    scanner.close();
                }
                catch (Throwable t2)
                {
                    JVMStabilityInspector.inspectThrowable(t2);
                    if (t == null)
                        t = t2;
                    else
                        t.addSuppressed(t2);
                }
            }
            if (t != null)
                throw Throwables.propagate(t);
        }
    }

    public ScannerList getScanners(Collection<SSTableReader> toCompact)
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

        //sstable range overlap check is disabled. See CASSANDRA-6563.
        if (uncheckedTombstoneCompaction)
            return true;

        Collection<SSTableReader> overlaps = cfs.getOverlappingSSTables(Collections.singleton(sstable));
        if (overlaps.isEmpty())
        {
            // there is no overlap, tombstones are safely droppable
            return true;
        }
        else if (CompactionController.getFullyExpiredSSTables(cfs, Collections.singleton(sstable), overlaps, gcBefore).size() > 0)
        {
            return true;
        }
        else
        {
            // what percentage of columns do we expect to compact outside of overlap?
            if (sstable.getIndexSummarySize() < 2)
            {
                // we have too few samples to estimate correct percentage
                return false;
            }
            // first, calculate estimated keys that do not overlap
            long keys = sstable.estimatedKeys();
            Set<Range<Token>> ranges = new HashSet<Range<Token>>(overlaps.size());
            for (SSTableReader overlap : overlaps)
                ranges.add(new Range<>(overlap.first.getToken(), overlap.last.getToken()));
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

        String unchecked = options.get(UNCHECKED_TOMBSTONE_COMPACTION_OPTION);
        if (unchecked != null)
        {
            if (!unchecked.equalsIgnoreCase("true") && !unchecked.equalsIgnoreCase("false"))
                throw new ConfigurationException(String.format("'%s' should be either 'true' or 'false', not '%s'",UNCHECKED_TOMBSTONE_COMPACTION_OPTION, unchecked));
        }

        String compactionEnabled = options.get(COMPACTION_ENABLED);
        if (compactionEnabled != null)
        {
            if (!compactionEnabled.equalsIgnoreCase("true") && !compactionEnabled.equalsIgnoreCase("false"))
            {
                throw new ConfigurationException(String.format("enabled should either be 'true' or 'false', not %s", compactionEnabled));
            }
        }
        Map<String, String> uncheckedOptions = new HashMap<String, String>(options);
        uncheckedOptions.remove(TOMBSTONE_THRESHOLD_OPTION);
        uncheckedOptions.remove(TOMBSTONE_COMPACTION_INTERVAL_OPTION);
        uncheckedOptions.remove(UNCHECKED_TOMBSTONE_COMPACTION_OPTION);
        uncheckedOptions.remove(COMPACTION_ENABLED);
        return uncheckedOptions;
    }

    public boolean shouldBeEnabled()
    {
        String optionValue = options.get(COMPACTION_ENABLED);

        return optionValue == null || Boolean.parseBoolean(optionValue);
    }


    /**
     * Method for grouping similar SSTables together, This will be used by
     * anti-compaction to determine which SSTables should be anitcompacted
     * as a group. If a given compaction strategy creates sstables which
     * cannot be merged due to some constraint it must override this method.
     */
    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        int groupSize = 2;
        List<SSTableReader> sortedSSTablesToGroup = new ArrayList<>(sstablesToGroup);
        Collections.sort(sortedSSTablesToGroup, SSTableReader.sstableComparator);

        Collection<Collection<SSTableReader>> groupedSSTables = new ArrayList<>();
        Collection<SSTableReader> currGroup = new ArrayList<>();

        for (SSTableReader sstable : sortedSSTablesToGroup)
        {
            currGroup.add(sstable);
            if (currGroup.size() == groupSize)
            {
                groupedSSTables.add(currGroup);
                currGroup = new ArrayList<>();
            }
        }

        if (currGroup.size() != 0)
            groupedSSTables.add(currGroup);
        return groupedSSTables;
    }
}
