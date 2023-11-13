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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SimpleSSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

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
    public abstract AbstractCompactionTask getNextBackgroundTask(final long gcBefore);

    /**
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    public abstract Collection<AbstractCompactionTask> getMaximalTask(final long gcBefore, boolean splitOutput);

    /**
     * @param sstables SSTables to compact. Must be marked as compacting.
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    public abstract AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final long gcBefore);

    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, final long gcBefore, long maxSSTableBytes)
    {
        return new CompactionTask(cfs, txn, gcBefore);
    }

    /**
     * @return the number of background tasks estimated to still be needed for this columnfamilystore
     */
    public abstract int getEstimatedRemainingTasks();

    /**
     * @return the estimated number of background tasks needed, assuming an additional number of SSTables
     */
    int getEstimatedRemainingTasks(int additionalSSTables, long additionalBytes)
    {
        return getEstimatedRemainingTasks() + (int)Math.ceil((double)additionalSSTables / cfs.getMaximumCompactionThreshold());
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
    public static List<SSTableReader> filterSuspectSSTables(Iterable<SSTableReader> originalCandidates)
    {
        List<SSTableReader> filtered = new ArrayList<>();
        for (SSTableReader sstable : originalCandidates)
        {
            if (!sstable.isMarkedSuspect())
                filtered.add(sstable);
        }
        return filtered;
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
    public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        ArrayList<ISSTableScanner> scanners = new ArrayList<>();
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

    /**
     * Replaces sstables in the compaction strategy
     *
     * Note that implementations must be able to handle duplicate notifications here (that removed are already gone and
     * added have already been added)
     * */
    public synchronized void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        for (SSTableReader remove : removed)
            removeSSTable(remove);
        addSSTables(added);
    }

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
     * Check if given sstable is worth dropping tombstones at gcBefore.
     * Check is skipped if tombstone_compaction_interval time does not elapse since sstable creation and returns false.
     *
     * @param sstable SSTable to check
     * @param gcBefore time to drop tombstones
     * @return true if given sstable's tombstones are expected to be removed
     */
    protected boolean worthDroppingTombstones(SSTableReader sstable, long gcBefore)
    {
        if (disableTombstoneCompactions || CompactionController.NEVER_PURGE_TOMBSTONES_PROPERTY_VALUE || cfs.getNeverPurgeTombstones())
            return false;
        // since we use estimations to calculate, there is a chance that compaction will not drop tombstones actually.
        // if that happens we will end up in infinite compaction loop, so first we check enough if enough time has
        // elapsed since SSTable created.
        if (currentTimeMillis() < sstable.getDataCreationTime() + tombstoneCompactionInterval * 1000)
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
            if (!sstable.isEstimationInformative())
            {
                // we have too few samples to estimate correct percentage
                return false;
            }
            // first, calculate estimated keys that do not overlap
            long keys = sstable.estimatedKeys();
            Set<Range<Token>> ranges = new HashSet<>(overlaps.size());
            for (SSTableReader overlap : overlaps)
                ranges.add(new Range<>(overlap.getFirst().getToken(), overlap.getLast().getToken()));
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
                                                       TimeUUID pendingRepair,
                                                       boolean isTransient,
                                                       IntervalSet<CommitLogPosition> commitLogPositions,
                                                       int sstableLevel,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return SimpleSSTableMultiWriter.create(descriptor,
                                               keyCount,
                                               repairedAt,
                                               pendingRepair,
                                               isTransient,
                                               cfs.metadata,
                                               commitLogPositions,
                                               sstableLevel,
                                               header,
                                               indexGroups,
                                               lifecycleNewTracker, cfs);
    }

    public boolean supportsEarlyOpen()
    {
        return true;
    }
}
