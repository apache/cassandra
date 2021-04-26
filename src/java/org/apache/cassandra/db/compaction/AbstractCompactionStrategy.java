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
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SimpleSSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;

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
    protected static final boolean DEFAULT_LOG_ALL_OPTION = false;

    protected static final String TOMBSTONE_THRESHOLD_OPTION = "tombstone_threshold";
    protected static final String TOMBSTONE_COMPACTION_INTERVAL_OPTION = "tombstone_compaction_interval";
    // disable range overlap check when deciding if an SSTable is candidate for tombstone compaction (CASSANDRA-6563)
    protected static final String UNCHECKED_TOMBSTONE_COMPACTION_OPTION = "unchecked_tombstone_compaction";
    protected static final String LOG_ALL_OPTION = "log_all";
    protected static final String COMPACTION_ENABLED = "enabled";
    public static final String ONLY_PURGE_REPAIRED_TOMBSTONES = "only_purge_repaired_tombstones";

    protected Map<String, String> options;

    protected final ColumnFamilyStore cfs;
    protected float tombstoneThreshold;
    protected long tombstoneCompactionInterval;
    protected boolean uncheckedTombstoneCompaction;
    protected boolean disableTombstoneCompactions = false;
    protected boolean logAll = true;

    private final Directories directories;

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

    /**
     * This class groups all the compaction tasks that are pending, submitted, in progress and completed.
     */
    protected final BackgroundCompactions backgroundCompactions;

    protected AbstractCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.options = ImmutableMap.copyOf(options);
        this.backgroundCompactions = new BackgroundCompactions(this, cfs);

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
            optionValue = options.get(LOG_ALL_OPTION);
            logAll = optionValue == null ? DEFAULT_LOG_ALL_OPTION : Boolean.parseBoolean(optionValue);
        }
        catch (ConfigurationException e)
        {
            logger.warn("Error setting compaction strategy options ({}), defaults will be used", e.getMessage());
            tombstoneThreshold = DEFAULT_TOMBSTONE_THRESHOLD;
            tombstoneCompactionInterval = DEFAULT_TOMBSTONE_COMPACTION_INTERVAL;
            uncheckedTombstoneCompaction = DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION_OPTION;
        }

        directories = cfs.getDirectories();
    }

    public BackgroundCompactions getBackgroundCompactions()
    {
        return backgroundCompactions;
    }

    public Directories getDirectories()
    {
        return directories;
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
    public abstract AbstractCompactionTask getNextBackgroundTask(int gcBefore);

    /**
     * Helper base class for strategies that provide CompactionAggregates, implementing the typical
     * getNextBackgroundTask logic based on a getNextBackgroundAggregate method.
     */
    protected static abstract class WithAggregates extends AbstractCompactionStrategy
    {
        protected WithAggregates(ColumnFamilyStore cfs, Map<String, String> options)
        {
            super(cfs, options);
        }

        @Override
        @SuppressWarnings("resource")
        public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
        {
            CompactionPick previous = null;
            while (true)
            {
                CompactionAggregate compaction = getNextBackgroundAggregate(gcBefore);
                if (compaction == null || compaction.isEmpty())
                    return null;

                // Already tried acquiring references without success. It means there is a race with
                // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
                if (compaction.getSelected().equals(previous))
                {
                    logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                                "unless it happens frequently, in which case it must be reported. Will retry later.",
                                compaction.getSelected());
                    return null;
                }

                LifecycleTransaction transaction = cfs.getTracker().tryModify(compaction.getSelected().sstables, OperationType.COMPACTION);
                if (transaction != null)
                {
                    backgroundCompactions.setSubmitted(transaction.opId(), compaction);
                    return createCompactionTask(gcBefore, transaction, compaction);
                }

                // Getting references to the sstables failed. This may be because we tried to compact sstables that are
                // no longer present (due to races in getting the notification), or because we still haven't
                // received any replace notifications. Remove any non-live sstables we track and try again.
                removeDeadSSTables();

                previous = compaction.getSelected();
            }
        }

        /**
         * Select the next compaction to perform. This method is typically synchronized.
         */
        protected abstract CompactionAggregate getNextBackgroundAggregate(int gcBefore);

        protected AbstractCompactionTask createCompactionTask(final int gcBefore, LifecycleTransaction txn, CompactionAggregate compaction)
        {
            return CompactionTask.forCompaction(this, txn, gcBefore);
        }

        @Override
        public int getEstimatedRemainingTasks()
        {
            return backgroundCompactions.getEstimatedRemainingTasks();
        }
    }

    /**
     * Helper base class for (older, deprecated) strategies that provide a list of tables to compact, implementing the
     * typical getNextBackgroundTask logic based on a getNextBackgroundSSTables method.
     */
    protected static abstract class WithSSTableList extends AbstractCompactionStrategy
    {
        protected WithSSTableList(ColumnFamilyStore cfs, Map<String, String> options)
        {
            super(cfs, options);
        }

        @Override
        @SuppressWarnings("resource")
        public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
        {
            List<SSTableReader> previousCandidate = null;
            while (true)
            {
                List<SSTableReader> latestBucket = getNextBackgroundSSTables(gcBefore);

                if (latestBucket.isEmpty())
                    return null;

                // Already tried acquiring references without success. It means there is a race with
                // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
                if (latestBucket.equals(previousCandidate))
                {
                    logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                                "unless it happens frequently, in which case it must be reported. Will retry later.",
                                latestBucket);
                    return null;
                }

                LifecycleTransaction modifier = cfs.getTracker().tryModify(latestBucket, OperationType.COMPACTION);
                if (modifier != null)
                    return createCompactionTask(gcBefore, modifier, false, false);

                // Getting references to the sstables failed. This may be because we tried to compact sstables that are
                // no longer present (due to races in getting the notification), or because we still haven't
                // received any replace notifications. Remove any non-live sstables we track and try again.
                removeDeadSSTables();

                previousCandidate = latestBucket;
            }
        }

        /**
         * Select the next tables to compact. This method is typically synchronized.
         */
        protected abstract List<SSTableReader> getNextBackgroundSSTables(final int gcBefore);
    }

    /**
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    @SuppressWarnings("resource")
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        removeDeadSSTables();

        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(getSSTables());
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        return Collections.singleton(createCompactionTask(gcBefore, txn, true, splitOutput));
    }

    /**
     * @param sstables SSTables to compact. Must be marked as compacting.
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    @SuppressWarnings("resource")
    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction modifier = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (modifier == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return createCompactionTask(gcBefore, modifier, false, false).setUserDefined(true);
    }

    /**
     * Create a compaction task for a maximal, user defined or background compaction without aggregates (legacy strategies).
     * Background compactions for strategies that extend {@link WithAggregates} will use
     * {@link WithAggregates#createCompactionTask(int, LifecycleTransaction, boolean, boolean)} instead.
     *
     * @param gcBefore tombstone threshold, older tombstones can be discarded
     * @param txn the transaction containing the files to be compacted
     * @param isMaximal set to true only when it's a maximal compaction
     * @param splitOutput false except for maximal compactions and passed in by the user to indicate to SizeTieredCompactionStrategy to split the out,
     *                    ignored otherwise
     *
     * @return a compaction task, see {@link AbstractCompactionTask} and sub-classes
     */
    protected AbstractCompactionTask createCompactionTask(final int gcBefore, LifecycleTransaction txn, boolean isMaximal, boolean splitOutput)
    {
        return CompactionTask.forCompaction(this, txn, gcBefore);
    }

    /**
     * Create a compaction task for operations that are not driven by the strategies.
     *
     * @param txn the transaction containing the files to be compacted
     * @param gcBefore tombstone threshold, older tombstones can be discarded
     * @param maxSSTableBytes the maximum size in bytes for an output sstables
     *
     * @return a compaction task, see {@link AbstractCompactionTask} and sub-classes
     */
    public AbstractCompactionTask createCompactionTask(LifecycleTransaction txn, final int gcBefore, long maxSSTableBytes)
    {
        return CompactionTask.forCompaction(this, txn, gcBefore);
    }

    /**
     * Get the estimated remaining compactions. Strategies that implement {@link WithAggregates} can delegate this
     * to {@link BackgroundCompactions} because they set the pending aggregates as background compactions but legacy
     * strategies that do not support aggregates must implement this method.
     * <p/>
     * @return the number of background tasks estimated to still be needed for this strategy
     */
    public abstract int getEstimatedRemainingTasks();

    /**
     * @return the total number of background compactions, pending or in progress
     */
    public int getTotalCompactions()
    {
        return getEstimatedRemainingTasks() + backgroundCompactions.getCompactionsInProgress();
    }

    /**
     * Return the statistics. Only strategies that implement {@link WithAggregates} will provide non-empty statistics,
     * the legacy strategies will always have empty statistics.
     * <p/>
     * @return statistics about this compaction picks.
     */
    public CompactionStrategyStatistics getStatistics()
    {
        return backgroundCompactions.getStatistics();
    }

    /**
     * @return size in bytes of the largest sstables for this strategy
     */
    public abstract long getMaxSSTableBytes();

    /**
     * Filters SSTables that are to be excluded from the given collection
     *
     * @param originalCandidates The collection to check for excluded SSTables
     * @return list of the SSTables with excluded ones filtered out
     */
    public static List<SSTableReader> filterSuspectSSTables(Iterable<? extends SSTableReader> originalCandidates)
    {
        List<SSTableReader> filtered = new ArrayList<>();
        for (SSTableReader sstable : originalCandidates)
        {
            if (!sstable.isMarkedSuspect())
                filtered.add(sstable);
        }
        return filtered;
    }

    public static Iterable<SSTableReader> nonSuspectAndNotIn(Iterable<SSTableReader> tables, Set<SSTableReader> compacting)
    {
        return Iterables.filter(tables, t -> !t.isMarkedSuspect() && !compacting.contains(t));
    }

    public ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        return range == null ? getScanners(sstables, (Collection<Range<Token>>)null) : getScanners(sstables, Collections.singleton(range));
    }

    /**
     * Returns a list of KeyScanners given sstables and a range on which to scan.
     * The default implementation simply grab one SSTableScanner per-sstable, but overriding this method
     * allow for a more memory efficient solution if we know the sstable don't overlap (see
     * LeveledCompactionStrategy for instance).
     */
    @SuppressWarnings("resource")
    public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        ArrayList<ISSTableScanner> scanners = new ArrayList<ISSTableScanner>();
        try
        {
            for (SSTableReader sstable : sstables)
                scanners.add(sstable.getScanner(ranges));
        }
        catch (Throwable t)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, t);
        }
        return new ScannerList(scanners);
    }

    public String getName()
    {
        return getClass().getSimpleName();
    }

    public TableMetadata getMetadata()
    {
        return cfs.metadata();
    }

    /**
     * Replaces sstables in the compaction strategy
     *
     * Note that implementations must be able to handle duplicate notifications here (that removed are already gone and
     * added have already been added)
     * */
    public abstract void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added);

    /**
     * Adds sstable, note that implementations must handle duplicate notifications here (added already being in the compaction strategy)
     */
    public abstract void addSSTable(SSTableReader added);

    /**
     * Adds sstables, note that implementations must handle duplicate notifications here (added already being in the compaction strategy)
     */
    public synchronized void addSSTables(Iterable<SSTableReader> added)
    {
        for (SSTableReader sstable : added)
            addSSTable(sstable);
    }

    /**
     * Remove any tracked sstable that is no longer in the live set. Note that because we get notifications after the
     * tracker is modified, anything we know of must be already in the live set -- if it is not, it has been removed
     * from there, and we either haven't received the removal notification yet, or we did and we messed it up (i.e.
     * we got it before the addition). The former is transient, but the latter can cause persistent problems, including
     * fully stopping compaction. In any case, we should remove any such sstables.
     * There are two special-case implementations of this in MemoryOnlyStrategy and LeveledManifest.
     */
    abstract void removeDeadSSTables();

    void removeDeadSSTables(Iterable<SSTableReader> sstables)
    {
        synchronized (sstables)
        {
            int removed = 0;
            Set<SSTableReader> liveSet = cfs.getLiveSSTables();
            for (Iterator<SSTableReader> it = sstables.iterator(); it.hasNext(); )
            {
                SSTableReader sstable = it.next();
                if (!liveSet.contains(sstable))
                {
                    it.remove();
                    ++removed;
                }
            }

            if (removed > 0)
                logger.debug("Removed {} dead sstables from the compactions tracked list.", removed);
        }
    }

    /**
     * Removes sstable from the strategy, implementations must be able to handle the sstable having already been removed.
     */
    public abstract void removeSSTable(SSTableReader sstable);

    /**
     * Removes sstables from the strategy, implementations must be able to handle the sstables having already been removed.
     */
    public void removeSSTables(Iterable<SSTableReader> removed)
    {
        for (SSTableReader sstable : removed)
            removeSSTable(sstable);
    }

    /**
     * Returns the sstables managed by this strategy instance
     */
    @VisibleForTesting
    protected abstract Set<SSTableReader> getSSTables();

    /**
     * Called when the metadata has changed for an sstable - for example if the level changed
     *
     * Not called when repair status changes (which is also metadata), because this results in the
     * sstable getting removed from the compaction strategy instance.
     *
     * @param oldMetadata
     * @param sstable
     */
    public void metadataChanged(StatsMetadata oldMetadata, SSTableReader sstable)
    {
    }

    public static class ScannerList implements AutoCloseable
    {
        public final List<ISSTableScanner> scanners;
        public ScannerList(List<ISSTableScanner> scanners)
        {
            this.scanners = scanners;
        }

        public long getTotalBytesScanned()
        {
            long bytesScanned = 0L;
            for (int i=0, isize=scanners.size(); i<isize; i++)
                bytesScanned += scanners.get(i).getBytesScanned();

            return bytesScanned;
        }

        public long getTotalCompressedSize()
        {
            long compressedSize = 0;
            for (int i=0, isize=scanners.size(); i<isize; i++)
                compressedSize += scanners.get(i).getCompressedLengthInBytes();

            return compressedSize;
        }

        public double getCompressionRatio()
        {
            double compressed = 0.0;
            double uncompressed = 0.0;

            for (int i=0, isize=scanners.size(); i<isize; i++)
            {
                @SuppressWarnings("resource")
                ISSTableScanner scanner = scanners.get(i);
                compressed += scanner.getCompressedLengthInBytes();
                uncompressed += scanner.getLengthInBytes();
            }

            if (compressed == uncompressed || uncompressed == 0)
                return MetadataCollector.NO_COMPRESSION_RATIO;

            return compressed / uncompressed;
        }

        public void close()
        {
            ISSTableScanner.closeAllAndPropagate(scanners, null);
        }
    }

    public ScannerList getScanners(Collection<SSTableReader> toCompact)
    {
        return getScanners(toCompact, (Collection<Range<Token>>)null);
    }

    /**
     * Select a table for tombstone-removing compaction from the given set. Returns null if no table is suitable.
     */
    @Nullable
    CompactionAggregate makeTombstoneCompaction(int gcBefore,
                                                Iterable<SSTableReader> candidates,
                                                Function<Collection<SSTableReader>, SSTableReader> selector)
    {
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return null;

        final SSTableReader sstable = selector.apply(sstablesWithTombstones);
        return CompactionAggregate.createForTombstones(sstable);
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
        if (disableTombstoneCompactions || CompactionController.NEVER_PURGE_TOMBSTONES || cfs.getNeverPurgeTombstones())
            return false;
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

        Collection<SSTableReader> overlaps = cfs.getOverlappingLiveSSTables(Collections.singleton(sstable));
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
            long columns = sstable.getEstimatedCellPerPartitionCount().mean() * remainingKeys;
            double remainingColumnsRatio = ((double) columns) / (sstable.getEstimatedCellPerPartitionCount().count() * sstable.getEstimatedCellPerPartitionCount().mean());

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
                throw new ConfigurationException(String.format("'%s' should be either 'true' or 'false', not '%s'", UNCHECKED_TOMBSTONE_COMPACTION_OPTION, unchecked));
        }

        String logAll = options.get(LOG_ALL_OPTION);
        if (logAll != null)
        {
            if (!logAll.equalsIgnoreCase("true") && !logAll.equalsIgnoreCase("false"))
            {
                throw new ConfigurationException(String.format("'%s' should either be 'true' or 'false', not %s", LOG_ALL_OPTION, logAll));
            }
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
        uncheckedOptions.remove(LOG_ALL_OPTION);
        uncheckedOptions.remove(COMPACTION_ENABLED);
        uncheckedOptions.remove(ONLY_PURGE_REPAIRED_TOMBSTONES);
        uncheckedOptions.remove(CompactionParams.Option.PROVIDE_OVERLAPPING_TOMBSTONES.toString());
        return uncheckedOptions;
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
        Collections.sort(sortedSSTablesToGroup, SSTableReader.firstKeyComparator);

        Collection<Collection<SSTableReader>> groupedSSTables = new ArrayList<>();
        Collection<SSTableReader> currGroup = new ArrayList<>(groupSize);

        for (SSTableReader sstable : sortedSSTablesToGroup)
        {
            currGroup.add(sstable);
            if (currGroup.size() == groupSize)
            {
                groupedSSTables.add(currGroup);
                currGroup = new ArrayList<>(groupSize);
            }
        }

        if (currGroup.size() != 0)
            groupedSSTables.add(currGroup);
        return groupedSSTables;
    }

    public CompactionLogger.Strategy strategyLogger()
    {
        return CompactionLogger.Strategy.none;
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       MetadataCollector meta,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return SimpleSSTableMultiWriter.create(descriptor, keyCount, repairedAt, pendingRepair, isTransient, cfs.metadata, meta, header, indexGroups, lifecycleNewTracker);
    }

    public boolean supportsEarlyOpen()
    {
        return true;
    }
}
