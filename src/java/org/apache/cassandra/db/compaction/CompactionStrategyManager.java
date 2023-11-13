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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.AbstractStrategyHolder.TaskSupplier;
import org.apache.cassandra.db.compaction.PendingRepairManager.CleanupTask;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableMetadataChanged;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.repair.consistent.admin.CleanupSummary;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.compaction.AbstractStrategyHolder.GroupedSSTableContainer;

/**
 * Manages the compaction strategies.
 *
 * SSTables are isolated from each other based on their incremental repair status (repaired, unrepaired, or pending repair)
 * and directory (determined by their starting token). This class handles the routing between {@link AbstractStrategyHolder}
 * instances based on repair status, and the {@link AbstractStrategyHolder} instances have separate compaction strategies
 * for each directory, which it routes sstables to. Note that {@link PendingRepairHolder} also divides sstables on their
 * pending repair id.
 *
 * Operations on this class are guarded by a {@link ReentrantReadWriteLock}. This lock performs mutual exclusion on
 * reads and writes to the following variables: {@link this#repaired}, {@link this#unrepaired}, {@link this#isActive},
 * {@link this#params}, {@link this#currentBoundaries}. Whenever performing reads on these variables,
 * the {@link this#readLock} should be acquired. Likewise, updates to these variables should be guarded by
 * {@link this#writeLock}.
 *
 * Whenever the {@link DiskBoundaries} change, the compaction strategies must be reloaded, so in order to ensure
 * the compaction strategy placement reflect most up-to-date disk boundaries, call {@link this#maybeReloadDiskBoundaries()}
 * before acquiring the read lock to acess the strategies.
 *
 */

public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    public final CompactionLogger compactionLogger;
    private final ColumnFamilyStore cfs;
    private final boolean partitionSSTablesByTokenRange;
    private final Supplier<DiskBoundaries> boundariesSupplier;

    /**
     * Performs mutual exclusion on the variables below
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    /**
     * Variables guarded by read and write lock above
     */
    private final PendingRepairHolder transientRepairs;
    private final PendingRepairHolder pendingRepairs;
    private final CompactionStrategyHolder repaired;
    private final CompactionStrategyHolder unrepaired;

    private final ImmutableList<AbstractStrategyHolder> holders;

    private volatile CompactionParams params;
    private DiskBoundaries currentBoundaries;
    private volatile boolean enabled;
    private volatile boolean isActive = true;

    /*
        We keep a copy of the schema compaction parameters here to be able to decide if we
        should update the compaction strategy in maybeReload() due to an ALTER.

        If a user changes the local compaction strategy and then later ALTERs a compaction parameter,
        we will use the new compaction parameters.
     */
    private volatile CompactionParams schemaCompactionParams;
    private volatile boolean supportsEarlyOpen;
    private volatile int fanout;
    private volatile long maxSSTableSizeBytes;
    private volatile String name;

    public static int TWCS_BUCKET_COUNT_MAX = 128;

    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        this(cfs, cfs::getDiskBoundaries, cfs.getPartitioner().splitter().isPresent());
    }

    @VisibleForTesting
    public CompactionStrategyManager(ColumnFamilyStore cfs, Supplier<DiskBoundaries> boundariesSupplier,
                                     boolean partitionSSTablesByTokenRange)
    {
        AbstractStrategyHolder.DestinationRouter router = new AbstractStrategyHolder.DestinationRouter()
        {
            public int getIndexForSSTable(SSTableReader sstable)
            {
                return compactionStrategyIndexFor(sstable);
            }

            public int getIndexForSSTableDirectory(Descriptor descriptor)
            {
                return compactionStrategyIndexForDirectory(descriptor);
            }
        };
        transientRepairs = new PendingRepairHolder(cfs, router, true);
        pendingRepairs = new PendingRepairHolder(cfs, router, false);
        repaired = new CompactionStrategyHolder(cfs, router, true);
        unrepaired = new CompactionStrategyHolder(cfs, router, false);
        holders = ImmutableList.of(transientRepairs, pendingRepairs, repaired, unrepaired);

        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
        this.cfs = cfs;
        this.compactionLogger = new CompactionLogger(cfs, this);
        this.boundariesSupplier = boundariesSupplier;
        this.partitionSSTablesByTokenRange = partitionSSTablesByTokenRange;

        currentBoundaries = boundariesSupplier.get();
        params = schemaCompactionParams = cfs.metadata().params.compaction;
        enabled = params.isEnabled();
        setStrategy(schemaCompactionParams);
        startup();
    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     */
    public AbstractCompactionTask getNextBackgroundTask(long gcBefore)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (!isEnabled())
                return null;

            int numPartitions = getNumTokenPartitions();

            // first try to promote/demote sstables from completed repairs
            AbstractCompactionTask repairFinishedTask;
            repairFinishedTask = pendingRepairs.getNextRepairFinishedTask();
            if (repairFinishedTask != null)
                return repairFinishedTask;

            repairFinishedTask = transientRepairs.getNextRepairFinishedTask();
            if (repairFinishedTask != null)
                return repairFinishedTask;

            // sort compaction task suppliers by remaining tasks descending
            List<TaskSupplier> suppliers = new ArrayList<>(numPartitions * holders.size());
            for (AbstractStrategyHolder holder : holders)
                suppliers.addAll(holder.getBackgroundTaskSuppliers(gcBefore));

            Collections.sort(suppliers);

            // return the first non-null task
            for (TaskSupplier supplier : suppliers)
            {
                AbstractCompactionTask task = supplier.getTask();
                if (task != null)
                    return task;
            }

            return null;
        }
        finally
        {
            readLock.unlock();
        }
    }

    /**
     * finds the oldest (by modification date) non-latest-version sstable on disk and creates an upgrade task for it
     * @return
     */
    @VisibleForTesting
    AbstractCompactionTask findUpgradeSSTableTask()
    {
        if (!isEnabled() || !DatabaseDescriptor.automaticSSTableUpgrade())
            return null;
        Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
        List<SSTableReader> potentialUpgrade = cfs.getLiveSSTables()
                                                  .stream()
                                                  .filter(s -> !compacting.contains(s) && !s.descriptor.version.isLatestVersion())
                                                  .sorted((o1, o2) -> {
                                                      File f1 = o1.descriptor.fileFor(Components.DATA);
                                                      File f2 = o2.descriptor.fileFor(Components.DATA);
                                                      return Longs.compare(f1.lastModified(), f2.lastModified());
                                                  }).collect(Collectors.toList());
        for (SSTableReader sstable : potentialUpgrade)
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UPGRADE_SSTABLES);
            if (txn != null)
            {
                logger.debug("Running automatic sstable upgrade for {}", sstable);
                return getCompactionStrategyFor(sstable).getCompactionTask(txn, Integer.MIN_VALUE, Long.MAX_VALUE);
            }
        }
        return null;
    }

    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    public boolean isActive()
    {
        return isActive;
    }

    public void resume()
    {
        writeLock.lock();
        try
        {
            isActive = true;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * pause compaction while we cancel all ongoing compactions
     *
     * Separate call from enable/disable to not have to save the enabled-state externally
      */
    public void pause()
    {
        writeLock.lock();
        try
        {
            isActive = false;
        }
        finally
        {
            writeLock.unlock();
        }

    }

    private void startup()
    {
        writeLock.lock();
        try
        {
            for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            {
                if (sstable.openReason != SSTableReader.OpenReason.EARLY)
                    compactionStrategyFor(sstable).addSSTable(sstable);
            }
            holders.forEach(AbstractStrategyHolder::startup);
            supportsEarlyOpen = repaired.first().supportsEarlyOpen();
            fanout = (repaired.first() instanceof LeveledCompactionStrategy) ? ((LeveledCompactionStrategy) repaired.first()).getLevelFanoutSize() : LeveledCompactionStrategy.DEFAULT_LEVEL_FANOUT_SIZE;
            maxSSTableSizeBytes = repaired.first().getMaxSSTableBytes();
            name = repaired.first().getName();
        }
        finally
        {
            writeLock.unlock();
        }

        if (repaired.first().logAll)
            compactionLogger.enable();
    }

    /**
     * return the compaction strategy for the given sstable
     *
     * returns differently based on the repaired status and which vnode the compaction strategy belongs to
     * @param sstable
     * @return
     */
    public AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        maybeReloadDiskBoundaries();
        return compactionStrategyFor(sstable);
    }

    @VisibleForTesting
    AbstractCompactionStrategy compactionStrategyFor(SSTableReader sstable)
    {
        // should not call maybeReloadDiskBoundaries because it may be called from within lock
        readLock.lock();
        try
        {
            return getHolder(sstable).getStrategyFor(sstable);
        }
        finally
        {
            readLock.unlock();
        }
    }

    /**
     * Get the correct compaction strategy for the given sstable. If the first token starts within a disk boundary, we
     * will add it to that compaction strategy.
     *
     * In the case we are upgrading, the first compaction strategy will get most files - we do not care about which disk
     * the sstable is on currently (unless we don't know the local tokens yet). Once we start compacting we will write out
     * sstables in the correct locations and give them to the correct compaction strategy instance.
     *
     * @param sstable
     * @return
     */
    int compactionStrategyIndexFor(SSTableReader sstable)
    {
        // should not call maybeReloadDiskBoundaries because it may be called from within lock
        readLock.lock();
        try
        {
            //We only have a single compaction strategy when sstables are not
            //partitioned by token range
            if (!partitionSSTablesByTokenRange)
                return 0;

            return currentBoundaries.getDiskIndex(sstable);
        }
        finally
        {
            readLock.unlock();
        }
    }

    private int compactionStrategyIndexForDirectory(Descriptor descriptor)
    {
        readLock.lock();
        try
        {
            return partitionSSTablesByTokenRange ? currentBoundaries.getBoundariesFromSSTableDirectory(descriptor) : 0;
        }
        finally
        {
            readLock.unlock();
        }
    }

    @VisibleForTesting
    CompactionStrategyHolder getRepairedUnsafe()
    {
        return repaired;
    }

    @VisibleForTesting
    CompactionStrategyHolder getUnrepairedUnsafe()
    {
        return unrepaired;
    }

    @VisibleForTesting
    PendingRepairHolder getPendingRepairsUnsafe()
    {
        return pendingRepairs;
    }

    @VisibleForTesting
    PendingRepairHolder getTransientRepairsUnsafe()
    {
        return transientRepairs;
    }

    public boolean hasDataForPendingRepair(TimeUUID sessionID)
    {
        readLock.lock();
        try
        {
            return pendingRepairs.hasDataForSession(sessionID) || transientRepairs.hasDataForSession(sessionID);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void shutdown()
    {
        writeLock.lock();
        try
        {
            isActive = false;
            holders.forEach(AbstractStrategyHolder::shutdown);
            compactionLogger.disable();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Maybe reload the compaction strategies. Called after changing configuration.
     */
    public void maybeReloadParamsFromSchema(CompactionParams params)
    {
        // compare the old schema configuration to the new one, ignore any locally set changes.
        if (params.equals(schemaCompactionParams))
            return;

        writeLock.lock();
        try
        {
            if (!params.equals(schemaCompactionParams))
                reloadParamsFromSchema(params);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * @param newParams new CompactionParams set in via CQL
     */
    private void reloadParamsFromSchema(CompactionParams newParams)
    {
        logger.debug("Recreating compaction strategy for {}.{} - compaction parameters changed via CQL",
                     cfs.getKeyspaceName(), cfs.getTableName());

        /*
         * It's possible for compaction to be explicitly enabled/disabled
         * via JMX when already enabled/disabled via params. In that case,
         * if we now toggle enabled/disabled via params, we'll technically
         * be overriding JMX-set value with params-set value.
         */
        boolean enabledWithJMX = enabled && !shouldBeEnabled();
        boolean disabledWithJMX = !enabled && shouldBeEnabled();

        schemaCompactionParams = newParams;
        setStrategy(newParams);

        // enable/disable via JMX overrides CQL params, but please see the comment above
        if (enabled && !shouldBeEnabled() && !enabledWithJMX)
            disable();
        else if (!enabled && shouldBeEnabled() && !disabledWithJMX)
            enable();

        startup();
    }

    private void maybeReloadParamsFromJMX(CompactionParams params)
    {
        // compare the old local configuration to the new one, ignoring schema
        if (params.equals(this.params))
            return;

        writeLock.lock();
        try
        {
            if (!params.equals(this.params))
                reloadParamsFromJMX(params);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * @param newParams new CompactionParams set via JMX
     */
    private void reloadParamsFromJMX(CompactionParams newParams)
    {
        logger.debug("Recreating compaction strategy for {}.{} - compaction parameters changed via JMX",
                     cfs.getKeyspaceName(), cfs.getTableName());

        setStrategy(newParams);

        // compaction params set via JMX override enable/disable via JMX
        if (enabled && !shouldBeEnabled())
            disable();
        else if (!enabled && shouldBeEnabled())
            enable();

        startup();
    }

    /**
     * Checks if the disk boundaries changed and reloads the compaction strategies
     * to reflect the most up-to-date disk boundaries.
     * <p>
     * This is typically called before acquiring the {@link this#readLock} to ensure the most up-to-date
     * disk locations and boundaries are used.
     * <p>
     * This should *never* be called inside by a thread holding the {@link this#readLock}, since it
     * will potentially acquire the {@link this#writeLock} to update the compaction strategies
     * what can cause a deadlock.
     * <p>
     * TODO: improve this to reload after receiving a notification rather than trying to reload on every operation
     */
    @VisibleForTesting
    protected void maybeReloadDiskBoundaries()
    {
        if (!currentBoundaries.isOutOfDate())
            return;

        writeLock.lock();
        try
        {
            if (currentBoundaries.isOutOfDate())
                reloadDiskBoundaries(boundariesSupplier.get());
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * @param newBoundaries new DiskBoundaries - potentially functionally equivalent to current ones
     */
    private void reloadDiskBoundaries(DiskBoundaries newBoundaries)
    {
        DiskBoundaries oldBoundaries = currentBoundaries;
        currentBoundaries = newBoundaries;

        if (newBoundaries.isEquivalentTo(oldBoundaries))
        {
            logger.debug("Not recreating compaction strategy for {}.{} - disk boundaries are equivalent",
                         cfs.getKeyspaceName(), cfs.getTableName());
            return;
        }

        logger.debug("Recreating compaction strategy for {}.{} - disk boundaries are out of date",
                     cfs.getKeyspaceName(), cfs.getTableName());
        setStrategy(params);
        startup();
    }

    private Iterable<AbstractCompactionStrategy> getAllStrategies()
    {
        return Iterables.concat(Iterables.transform(holders, AbstractStrategyHolder::allStrategies));
    }

    public int getUnleveledSSTables()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (repaired.first() instanceof LeveledCompactionStrategy)
            {
                int count = 0;
                for (AbstractCompactionStrategy strategy : getAllStrategies())
                    count += ((LeveledCompactionStrategy) strategy).getLevelSize(0);
                return count;
            }
        }
        finally
        {
            readLock.unlock();
        }
        return 0;
    }

    public int getLevelFanoutSize()
    {
        return fanout;
    }

    public int[] getSSTableCountPerLevel()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (repaired.first() instanceof LeveledCompactionStrategy)
            {
                int[] res = new int[LeveledGenerations.MAX_LEVEL_COUNT];
                for (AbstractCompactionStrategy strategy : getAllStrategies())
                {
                    int[] repairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                    res = sumArrays(res, repairedCountPerLevel);
                }
                return res;
            }
        }
        finally
        {
            readLock.unlock();
        }
        return null;
    }

    public long[] getPerLevelSizeBytes()
    {
        readLock.lock();
        try
        {
            if (repaired.first() instanceof LeveledCompactionStrategy)
            {
                long [] res = new long[LeveledGenerations.MAX_LEVEL_COUNT];
                for (AbstractCompactionStrategy strategy : getAllStrategies())
                {
                    long[] repairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSizeBytes();
                    res = sumArrays(res, repairedCountPerLevel);
                }
                return res;
            }
            return null;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean isLeveledCompaction()
    {
        readLock.lock();
        try
        {
            return repaired.first() instanceof LeveledCompactionStrategy;
        } finally
        {
            readLock.unlock();
        }
    }

    public int[] getSSTableCountPerTWCSBucket()
    {
        readLock.lock();
        try
        {
            List<Map<Long, Integer>> countsByBucket = Stream.concat(
                                                                StreamSupport.stream(repaired.allStrategies().spliterator(), false),
                                                                StreamSupport.stream(unrepaired.allStrategies().spliterator(), false))
                                                            .filter((TimeWindowCompactionStrategy.class)::isInstance)
                                                            .map(s -> ((TimeWindowCompactionStrategy)s).getSSTableCountByBuckets())
                                                            .collect(Collectors.toList());
            return countsByBucket.isEmpty() ? null : sumCountsByBucket(countsByBucket, TWCS_BUCKET_COUNT_MAX);
        }
        finally
        {
            readLock.unlock();
        }
    }

    static int[] sumCountsByBucket(List<Map<Long, Integer>> countsByBucket, int max)
    {
        TreeMap<Long, Integer> merged = new TreeMap<>(Comparator.reverseOrder());
        countsByBucket.stream().flatMap(e -> e.entrySet().stream()).forEach(e -> merged.merge(e.getKey(), e.getValue(), Integer::sum));
        return merged.values().stream().limit(max).mapToInt(i -> i).toArray();
    }

    static int[] sumArrays(int[] a, int[] b)
    {
        int[] res = new int[Math.max(a.length, b.length)];
        for (int i = 0; i < res.length; i++)
        {
            if (i < a.length && i < b.length)
                res[i] = a[i] + b[i];
            else if (i < a.length)
                res[i] = a[i];
            else
                res[i] = b[i];
        }
        return res;
    }

    static long[] sumArrays(long[] a, long[] b)
    {
        long[] res = new long[Math.max(a.length, b.length)];
        for (int i = 0; i < res.length; i++)
        {
            if (i < a.length && i < b.length)
                res[i] = a[i] + b[i];
            else if (i < a.length)
                res[i] = a[i];
            else
                res[i] = b[i];
        }
        return res;
    }

    /**
     * Should only be called holding the readLock
     */
    private void handleFlushNotification(Iterable<SSTableReader> added)
    {
        for (SSTableReader sstable : added)
            compactionStrategyFor(sstable).addSSTable(sstable);
    }

    private int getHolderIndex(SSTableReader sstable)
    {
        for (int i = 0; i < holders.size(); i++)
        {
            if (holders.get(i).managesSSTable(sstable))
                return i;
        }

        throw new IllegalStateException("No holder claimed " + sstable);
    }

    private AbstractStrategyHolder getHolder(SSTableReader sstable)
    {
        for (AbstractStrategyHolder holder : holders)
        {
            if (holder.managesSSTable(sstable))
                return holder;
        }

        throw new IllegalStateException("No holder claimed " + sstable);
    }

    private AbstractStrategyHolder getHolder(long repairedAt, TimeUUID pendingRepair, boolean isTransient)
    {
        return getHolder(repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE,
                         pendingRepair != ActiveRepairService.NO_PENDING_REPAIR,
                         isTransient);
    }

    @VisibleForTesting
    AbstractStrategyHolder getHolder(boolean isRepaired, boolean isPendingRepair, boolean isTransient)
    {
        for (AbstractStrategyHolder holder : holders)
        {
            if (holder.managesRepairedGroup(isRepaired, isPendingRepair, isTransient))
                return holder;
        }

        throw new IllegalStateException(String.format("No holder claimed isPendingRepair: %s, isPendingRepair %s",
                                                      isRepaired, isPendingRepair));
    }

    @VisibleForTesting
    ImmutableList<AbstractStrategyHolder> getHolders()
    {
        return holders;
    }

    /**
     * Split sstables into a list of grouped sstable containers, the list index an sstable
     *
     * lives in matches the list index of the holder that's responsible for it
     */
    public List<GroupedSSTableContainer> groupSSTables(Iterable<SSTableReader> sstables)
    {
        List<GroupedSSTableContainer> classified = new ArrayList<>(holders.size());
        for (AbstractStrategyHolder holder : holders)
        {
            classified.add(holder.createGroupedSSTableContainer());
        }

        for (SSTableReader sstable : sstables)
        {
            classified.get(getHolderIndex(sstable)).add(sstable);
        }

        return classified;
    }

    /**
     * Should only be called holding the readLock
     */
    private void handleListChangedNotification(Iterable<SSTableReader> added, Iterable<SSTableReader> removed)
    {
        List<GroupedSSTableContainer> addedGroups = groupSSTables(added);
        List<GroupedSSTableContainer> removedGroups = groupSSTables(removed);
        for (int i=0; i<holders.size(); i++)
        {
            holders.get(i).replaceSSTables(removedGroups.get(i), addedGroups.get(i));
        }
    }

    /**
     * Should only be called holding the readLock
     */
    private void handleRepairStatusChangedNotification(Iterable<SSTableReader> sstables)
    {
        List<GroupedSSTableContainer> groups = groupSSTables(sstables);
        for (int i = 0; i < holders.size(); i++)
        {
            GroupedSSTableContainer group = groups.get(i);

            if (group.isEmpty())
                continue;

            AbstractStrategyHolder dstHolder = holders.get(i);
            for (AbstractStrategyHolder holder : holders)
            {
                if (holder != dstHolder)
                    holder.removeSSTables(group);
            }

            // adding sstables into another strategy may change its level,
            // thus it won't be removed from original LCS. We have to remove sstables first
            dstHolder.addSSTables(group);
        }
    }

    /**
     * Should only be called holding the readLock
     */
    private void handleMetadataChangedNotification(SSTableReader sstable, StatsMetadata oldMetadata)
    {
        compactionStrategyFor(sstable).metadataChanged(oldMetadata, sstable);
    }

    /**
     * Should only be called holding the readLock
     */
    private void handleDeletingNotification(SSTableReader deleted)
    {
        compactionStrategyFor(deleted).removeSSTable(deleted);
    }

    public void handleNotification(INotification notification, Object sender)
    {
        // we might race with reload adding/removing the sstables, this means that compaction strategies
        // must handle double notifications.
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {

            if (notification instanceof SSTableAddedNotification)
            {
                SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
                handleFlushNotification(flushedNotification.added);
            }
            else if (notification instanceof SSTableListChangedNotification)
            {
                SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
                handleListChangedNotification(listChangedNotification.added, listChangedNotification.removed);
            }
            else if (notification instanceof SSTableRepairStatusChanged)
            {
                handleRepairStatusChangedNotification(((SSTableRepairStatusChanged) notification).sstables);
            }
            else if (notification instanceof SSTableDeletingNotification)
            {
                handleDeletingNotification(((SSTableDeletingNotification) notification).deleting);
            }
            else if (notification instanceof SSTableMetadataChanged)
            {
                SSTableMetadataChanged lcNotification = (SSTableMetadataChanged) notification;
                handleMetadataChangedNotification(lcNotification.sstable, lcNotification.oldMetadata);
            }
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void enable()
    {
        writeLock.lock();
        try
        {
            // enable this last to make sure the strategies are ready to get calls.
            enabled = true;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void disable()
    {
        writeLock.lock();
        try
        {
            enabled = false;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Create ISSTableScanners from the given sstables
     *
     * Delegates the call to the compaction strategies to allow LCS to create a scanner
     * @param sstables
     * @param ranges
     * @return
     */
    public AbstractCompactionStrategy.ScannerList maybeGetScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        maybeReloadDiskBoundaries();
        List<ISSTableScanner> scanners = new ArrayList<>(sstables.size());
        readLock.lock();
        try
        {
            List<GroupedSSTableContainer> sstableGroups = groupSSTables(sstables);

            for (int i = 0; i < holders.size(); i++)
            {
                AbstractStrategyHolder holder = holders.get(i);
                GroupedSSTableContainer group = sstableGroups.get(i);
                scanners.addAll(holder.getScanners(group, ranges));
            }
        }
        catch (PendingRepairManager.IllegalSSTableArgumentException e)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, new ConcurrentModificationException(e));
        }
        finally
        {
            readLock.unlock();
        }
        return new AbstractCompactionStrategy.ScannerList(scanners);
    }

    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        while (true)
        {
            try
            {
                return maybeGetScanners(sstables, ranges);
            }
            catch (ConcurrentModificationException e)
            {
                logger.debug("SSTable repairedAt/pendingRepaired values changed while getting scanners");
            }
        }
    }

    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, null);
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            return unrepaired.groupForAnticompaction(sstablesToGroup);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public long getMaxSSTableBytes()
    {
        return maxSSTableSizeBytes;
    }

    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, long gcBefore, long maxSSTableBytes)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            validateForCompaction(txn.originals());
            return compactionStrategyFor(txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
        }
        finally
        {
            readLock.unlock();
        }

    }

    private void validateForCompaction(Iterable<SSTableReader> input)
    {
        readLock.lock();
        try
        {
            SSTableReader firstSSTable = Iterables.getFirst(input, null);
            assert firstSSTable != null;
            boolean repaired = firstSSTable.isRepaired();
            int firstIndex = compactionStrategyIndexFor(firstSSTable);
            boolean isPending = firstSSTable.isPendingRepair();
            TimeUUID pendingRepair = firstSSTable.getSSTableMetadata().pendingRepair;
            for (SSTableReader sstable : input)
            {
                if (sstable.isRepaired() != repaired)
                    throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
                if (firstIndex != compactionStrategyIndexFor(sstable))
                    throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
                if (isPending && !pendingRepair.equals(sstable.getSSTableMetadata().pendingRepair))
                    throw new UnsupportedOperationException("You can't compact sstables from different pending repair sessions");
            }
        }
        finally
        {
            readLock.unlock();
        }
    }

    public CompactionTasks getMaximalTasks(final long gcBefore, final boolean splitOutput, OperationType operationType)
    {
        maybeReloadDiskBoundaries();
        // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired strategies mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(() -> {
            List<AbstractCompactionTask> tasks = new ArrayList<>();
            readLock.lock();
            try
            {
                for (AbstractStrategyHolder holder : holders)
                {
                    for (AbstractCompactionTask task: holder.getMaximalTasks(gcBefore, splitOutput))
                    {
                        tasks.add(task.setCompactionType(operationType));
                    }
                }
            }
            finally
            {
                readLock.unlock();
            }
            return CompactionTasks.create(tasks);
        }, operationType, false, false);
    }

    /**
     * Return a list of compaction tasks corresponding to the sstables requested. Split the sstables according
     * to whether they are repaired or not, and by disk location. Return a task per disk location and repair status
     * group.
     *
     * @param sstables the sstables to compact
     * @param gcBefore gc grace period, throw away tombstones older than this
     * @return a list of compaction tasks corresponding to the sstables requested
     */
    public CompactionTasks getUserDefinedTasks(Collection<SSTableReader> sstables, long gcBefore)
    {
        maybeReloadDiskBoundaries();
        List<AbstractCompactionTask> ret = new ArrayList<>();
        readLock.lock();
        try
        {
            List<GroupedSSTableContainer> groupedSSTables = groupSSTables(sstables);
            for (int i = 0; i < holders.size(); i++)
            {
                ret.addAll(holders.get(i).getUserDefinedTasks(groupedSSTables.get(i), gcBefore));
            }
            return CompactionTasks.create(ret);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public int getEstimatedRemainingTasks()
    {
        maybeReloadDiskBoundaries();
        int tasks = 0;
        readLock.lock();
        try
        {
            for (AbstractCompactionStrategy strategy : getAllStrategies())
                tasks += strategy.getEstimatedRemainingTasks();
        }
        finally
        {
            readLock.unlock();
        }
        return tasks;
    }

    public int getEstimatedRemainingTasks(int additionalSSTables, long additionalBytes, boolean isIncremental)
    {
        if (additionalBytes == 0 || additionalSSTables == 0)
            return getEstimatedRemainingTasks();

        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            int tasks = pendingRepairs.getEstimatedRemainingTasks();

            Iterable<AbstractCompactionStrategy> strategies;
            if (isIncremental)
            {
                // Note that it is unlikely that we are behind in the pending strategies (as they only have a small fraction
                // of the total data), so we assume here that any pending sstables go directly to the repaired bucket.
                strategies = repaired.allStrategies();
                tasks += unrepaired.getEstimatedRemainingTasks();
            }
            else
            {
                // Here we assume that all sstables go to unrepaired, which can be wrong if we are running
                // both incremental and full repairs.
                strategies = unrepaired.allStrategies();
                tasks += repaired.getEstimatedRemainingTasks();

            }
            int strategyCount = Math.max(1, Iterables.size(strategies));
            int sstablesPerStrategy = additionalSSTables / strategyCount;
            long bytesPerStrategy = additionalBytes / strategyCount;
            for (AbstractCompactionStrategy strategy : strategies)
                tasks += strategy.getEstimatedRemainingTasks(sstablesPerStrategy, bytesPerStrategy);
            return tasks;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean shouldBeEnabled()
    {
        return params.isEnabled();
    }

    public String getName()
    {
        return name;
    }

    public List<List<AbstractCompactionStrategy>> getStrategies()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            return Arrays.asList(Lists.newArrayList(repaired.allStrategies()),
                                 Lists.newArrayList(unrepaired.allStrategies()),
                                 Lists.newArrayList(pendingRepairs.allStrategies()));
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void overrideLocalParams(CompactionParams params)
    {
        logger.info("Switching local compaction strategy from {} to {}", this.params, params);
        maybeReloadParamsFromJMX(params);
    }

    private int getNumTokenPartitions()
    {
        return partitionSSTablesByTokenRange ? currentBoundaries.directories.size() : 1;
    }

    private void setStrategy(CompactionParams params)
    {
        int numPartitions = getNumTokenPartitions();
        for (AbstractStrategyHolder holder : holders)
            holder.setStrategy(params, numPartitions);
        this.params = params;
    }

    public CompactionParams getCompactionParams()
    {
        return params;
    }

    public boolean onlyPurgeRepairedTombstones()
    {
        return Boolean.parseBoolean(params.options().get(AbstractCompactionStrategy.ONLY_PURGE_REPAIRED_TOMBSTONES));
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
        SSTable.validateRepairedMetadata(repairedAt, pendingRepair, isTransient);
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            return getHolder(repairedAt, pendingRepair, isTransient).createSSTableMultiWriter(descriptor,
                                                                                              keyCount,
                                                                                              repairedAt,
                                                                                              pendingRepair,
                                                                                              isTransient,
                                                                                              commitLogPositions,
                                                                                              sstableLevel,
                                                                                              header,
                                                                                              indexGroups,
                                                                                              lifecycleNewTracker);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean isRepaired(AbstractCompactionStrategy strategy)
    {
        return repaired.getStrategyIndex(strategy) >= 0;
    }

    public List<String> getStrategyFolders(AbstractCompactionStrategy strategy)
    {
        readLock.lock();
        try
        {
            Directories.DataDirectory[] locations = cfs.getDirectories().getWriteableLocations();
            if (partitionSSTablesByTokenRange)
            {
                for (AbstractStrategyHolder holder : holders)
                {
                    int idx = holder.getStrategyIndex(strategy);
                    if (idx >= 0)
                        return Collections.singletonList(locations[idx].location.absolutePath());
                }
            }
            List<String> folders = new ArrayList<>(locations.length);
            for (Directories.DataDirectory location : locations)
            {
                folders.add(location.location.absolutePath());
            }
            return folders;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean supportsEarlyOpen()
    {
        return supportsEarlyOpen;
    }

    @VisibleForTesting
    List<PendingRepairManager> getPendingRepairManagers()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            return Lists.newArrayList(pendingRepairs.getManagers());
        }
        finally
        {
            readLock.unlock();
        }
    }

    /**
     * Mutates sstable repairedAt times and notifies listeners of the change with the writeLock held. Prevents races
     * with other processes between when the metadata is changed and when sstables are moved between strategies.
      */
    public void mutateRepaired(Collection<SSTableReader> sstables, long repairedAt, TimeUUID pendingRepair, boolean isTransient) throws IOException
    {
        Set<SSTableReader> changed = new HashSet<>();

        writeLock.lock();
        try
        {
            for (SSTableReader sstable: sstables)
            {
                sstable.mutateRepairedAndReload(repairedAt, pendingRepair, isTransient);
                verifyMetadata(sstable, repairedAt, pendingRepair, isTransient);
                changed.add(sstable);
            }
        }
        finally
        {
            try
            {
                // if there was an exception mutating repairedAt, we should still notify for the
                // sstables that we were able to modify successfully before releasing the lock
                cfs.getTracker().notifySSTableRepairedStatusChanged(changed);
            }
            finally
            {
                writeLock.unlock();
            }
        }
    }

    private static void verifyMetadata(SSTableReader sstable, long repairedAt, TimeUUID pendingRepair, boolean isTransient)
    {
        if (!Objects.equals(pendingRepair, sstable.getPendingRepair()))
            throw new IllegalStateException(String.format("Failed setting pending repair to %s on %s (pending repair is %s)", pendingRepair, sstable, sstable.getPendingRepair()));
        if (repairedAt != sstable.getRepairedAt())
            throw new IllegalStateException(String.format("Failed setting repairedAt to %d on %s (repairedAt is %d)", repairedAt, sstable, sstable.getRepairedAt()));
        if (isTransient != sstable.isTransient())
            throw new IllegalStateException(String.format("Failed setting isTransient to %b on %s (isTransient is %b)", isTransient, sstable, sstable.isTransient()));
    }

    public CleanupSummary releaseRepairData(Collection<TimeUUID> sessions)
    {
        List<CleanupTask> cleanupTasks = new ArrayList<>();
        readLock.lock();
        try
        {
            for (PendingRepairManager prm : Iterables.concat(pendingRepairs.getManagers(), transientRepairs.getManagers()))
                cleanupTasks.add(prm.releaseSessionData(sessions));
        }
        finally
        {
            readLock.unlock();
        }

        CleanupSummary summary = new CleanupSummary(cfs, Collections.emptySet(), Collections.emptySet());

        for (CleanupTask task : cleanupTasks)
            summary = CleanupSummary.add(summary, task.cleanup());

        return summary;
    }
}
