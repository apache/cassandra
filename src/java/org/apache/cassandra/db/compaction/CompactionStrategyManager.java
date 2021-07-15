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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.AbstractStrategyHolder.TasksSupplier;
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
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableMetadataChanged;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;

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
 * before acquiring the read lock to access the strategies.
 *
 */

public class CompactionStrategyManager implements CompactionStrategyContainer
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
        We keep a copy of the table metadata compaction parameters here to be able to decide if we
        should update the compaction strategy due to a metadata change such as a schema changed
        caused by an ALTER TABLE.

        If a user changes the local compaction strategy via JMX and then later ALTERs a compaction parameter,
        we will use the new compaction parameters but we will not override the JMX parameters if compaction
        was not changed by the ALTER.
     */
    @SuppressWarnings("thread-safe")
    private volatile CompactionParams metadataParams;
    private volatile boolean supportsEarlyOpen;
    private volatile int fanout;
    private volatile long maxSSTableSizeBytes;
    private volatile String name;

    public CompactionStrategyManager(CompactionStrategyFactory strategyFactory)
    {
        this(strategyFactory,
             () -> strategyFactory.getCfs().getDiskBoundaries(),
             strategyFactory.getCfs().getPartitioner().splitter().isPresent());
    }

    @VisibleForTesting
    public CompactionStrategyManager(CompactionStrategyFactory strategyFactory,
                                     Supplier<DiskBoundaries> boundariesSupplier,
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

        cfs = strategyFactory.getCfs();

        transientRepairs = new PendingRepairHolder(cfs, strategyFactory, router, true);
        pendingRepairs = new PendingRepairHolder(cfs, strategyFactory, router, false);
        repaired = new CompactionStrategyHolder(cfs, strategyFactory, router, true);
        unrepaired = new CompactionStrategyHolder(cfs, strategyFactory, router, false);
        holders = ImmutableList.of(transientRepairs, pendingRepairs, repaired, unrepaired);

        compactionLogger = strategyFactory.getCompactionLogger();
        this.boundariesSupplier = boundariesSupplier;
        this.partitionSSTablesByTokenRange = partitionSSTablesByTokenRange;
        params = cfs.metadata().params.compaction;
        enabled = params.isEnabled();
    }

    public static CompactionStrategyContainer create(@Nullable CompactionStrategyContainer previous,
                                                     CompactionStrategyFactory strategyFactory,
                                                     CompactionParams compactionParams,
                                                     CompactionStrategyContainer.ReloadReason reason)
    {
        CompactionStrategyManager csm = new CompactionStrategyManager(strategyFactory);
        csm.reload(previous != null ? previous : csm, compactionParams, reason);
        return csm;
    }

    /**
     * Return the next background task
     *
     * Legacy strategies will always return one task but we wrap this in a collection because new strategies
     * might return multiple tasks.
     *
     * @return the task for the compaction strategy that needs it the most (most estimated remaining tasks)     */
    @Override
    public Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (!isEnabled())
                return ImmutableList.of();

            int numPartitions = getNumTokenPartitions();

            // first try to promote/demote sstables from completed repairs
            Collection<AbstractCompactionTask> repairFinishedTasks;
            repairFinishedTasks = pendingRepairs.getNextRepairFinishedTasks();
            if (!repairFinishedTasks.isEmpty())
                return repairFinishedTasks;

            repairFinishedTasks = transientRepairs.getNextRepairFinishedTasks();
            if (!repairFinishedTasks.isEmpty())
                return repairFinishedTasks;

            // sort compaction task suppliers by remaining tasks descending
            List<TasksSupplier> suppliers = new ArrayList<>(numPartitions * holders.size());
            for (AbstractStrategyHolder holder : holders)
                suppliers.addAll(holder.getBackgroundTaskSuppliers(gcBefore));

            Collections.sort(suppliers);

            // return the first non-empty list, we could enhance it to return all tasks of all
            // suppliers but this would change existing behavior
            for (TasksSupplier supplier : suppliers)
            {
                Collection<AbstractCompactionTask> tasks = supplier.getTasks();
                if (!tasks.isEmpty())
                    return tasks;
            }

            return ImmutableList.of();
        }
        finally
        {
            readLock.unlock();
        }
    }

    @Override
    public CompactionLogger getCompactionLogger()
    {
        return compactionLogger;
    }

    @Override
    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    @Override
    public boolean isActive()
    {
        return isActive;
    }

    @Override
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
    @Override
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

    @Override
    public void startup()
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
            maxSSTableSizeBytes = repaired.first().getMaxSSTableBytes();
            name = repaired.first().getName();
        }
        finally
        {
            writeLock.unlock();
        }

        if (repaired.first().getOptions().isLogAll())
            compactionLogger.enable();
    }

    /**
     * returns differently based on the repaired status and which vnode the compaction strategy belongs to
     * @param sstable
     * @return the compaction strategy for the given sstable
     */
    LegacyAbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        maybeReloadDiskBoundaries();
        return compactionStrategyFor(sstable);
    }

    @VisibleForTesting
    LegacyAbstractCompactionStrategy compactionStrategyFor(SSTableReader sstable)
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

            return currentBoundaries.getDiskIndexFromKey(sstable);
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

    @Override
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
     * Checks if the disk boundaries changed and reloads the compaction strategies
     * to reflect the most up-to-date disk boundaries.
     *
     * This is typically called before acquiring the {@link this#readLock} to ensure the most up-to-date
     * disk locations and boundaries are used.
     *
     * This should *never* be called inside by a thread holding the {@link this#readLock}, since it
     * will potentially acquire the {@link this#writeLock} to update the compaction strategies
     * what can cause a deadlock.
     */
    //TODO improve this to reload after receiving a notification rather than trying to reload on every operation
    @VisibleForTesting
    protected void maybeReloadDiskBoundaries()
    {
        if (!currentBoundaries.isOutOfDate())
            return;

        writeLock.lock();
        try
        {
            if (!currentBoundaries.isOutOfDate())
                return;
            doReload(this, params, ReloadReason.DISK_BOUNDARIES_UPDATED);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    @Override
    public CompactionStrategyContainer reload(@Nonnull CompactionStrategyContainer previous, CompactionParams newCompactionParams, ReloadReason reason)
    {
        writeLock.lock();
        try
        {
            doReload(previous, newCompactionParams, reason);
        }
        finally
        {
            writeLock.unlock();
        }
        if (previous != this)
            previous.shutdown();

        return this;
    }

    private void doReload(CompactionStrategyContainer previous, CompactionParams compactionParams, ReloadReason reason)
    {
        boolean updateDiskBoundaries = currentBoundaries == null || currentBoundaries.isOutOfDate();
        boolean enabledOnReload = CompactionStrategyFactory.enableCompactionOnReload(previous, compactionParams, reason);

        logger.debug("Recreating compaction strategy for {}.{}, reason: {}, params updated: {}, disk boundaries updated: {}, enabled: {}, params: {} -> {}, metadataParams: {}",
                     cfs.getKeyspaceName(), cfs.getTableName(), reason, !compactionParams.equals(params), updateDiskBoundaries, enabledOnReload, params, compactionParams, metadataParams);

        if (updateDiskBoundaries)
            currentBoundaries = boundariesSupplier.get();

        int numPartitions = getNumTokenPartitions();
        for (AbstractStrategyHolder holder : holders)
            holder.setStrategy(compactionParams, numPartitions);

        params = compactionParams;

        // full reload or switch from a strategy not managed by CompactionStrategyManager
        if (metadataParams == null || reason == ReloadReason.FULL)
            metadataParams = cfs.metadata().params.compaction;
        else if (reason == ReloadReason.METADATA_CHANGE)
            // metadataParams are aligned with compactionParams. We do not access TableParams.COMPACTION to avoid racing with
            // concurrent ALTER TABLE metadata change.
            metadataParams = compactionParams;

        // no-op for DISK_BOUNDARIES_UPDATED and JMX_REQUEST. DISK_BOUNDARIES_UPDATED does not change compaction params
        // and JMX changes do not affect table metadata


        if (params.maxCompactionThreshold() <= 0 || params.minCompactionThreshold() <= 0)
        {
            logger.warn("Disabling compaction strategy by setting compaction thresholds to 0 is deprecated, set the compaction option 'enabled' to 'false' instead.");
            disable();
        }
        else if (!enabledOnReload)
            disable();
        else
            enable();

        startup();
    }

    private Iterable<CompactionStrategy> getAllStrategies()
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
                for (CompactionStrategy strategy : getAllStrategies())
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

    @Override
    public int getLevelFanoutSize()
    {
        return repaired.first().getLevelFanoutSize();
    }

    @Override
    public int[] getSSTableCountPerLevel()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (repaired.first() instanceof LeveledCompactionStrategy)
            {
                int[] res = new int[LeveledGenerations.MAX_LEVEL_COUNT];
                for (CompactionStrategy strategy : getAllStrategies())
                {
                    int[] repairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                    res = sumArrays(res, repairedCountPerLevel);
                }
                return res;
            }
            else
            {
                return new int[0];
            }
        }
        finally
        {
            readLock.unlock();
        }
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

    private AbstractStrategyHolder getHolder(long repairedAt, UUID pendingRepair, boolean isTransient)
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
        LegacyAbstractCompactionStrategy acs = getCompactionStrategyFor(sstable);
        acs.metadataChanged(oldMetadata, sstable);
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

    @Override
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

    @Override
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
    @SuppressWarnings("resource")
    private ScannerList maybeGetScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
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
        return new ScannerList(scanners);
    }

    @Override
    public ScannerList getScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
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

    @Override
    public ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, null);
    }

    @Override
    public Set<SSTableReader> getSSTables()
    {
        return getStrategies().stream()
                              .flatMap(strategy -> strategy.getSSTables().stream())
                              .collect(Collectors.toSet());
    }

    @Override
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

    @Override
    public long getMaxSSTableBytes()
    {
        return maxSSTableSizeBytes;
    }

    @Override
    public AbstractCompactionTask createCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            validateForCompaction(txn.originals());
            return compactionStrategyFor(txn.originals().iterator().next()).createCompactionTask(txn, gcBefore, maxSSTableBytes);
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
            UUID pendingRepair = firstSSTable.getSSTableMetadata().pendingRepair;
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

    @Override
    public CompactionTasks getMaximalTasks(final int gcBefore, final boolean splitOutput)
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
                    tasks.addAll(holder.getMaximalTasks(gcBefore, splitOutput));
                }
            }
            finally
            {
                readLock.unlock();
            }
            return CompactionTasks.create(tasks);
        }, false, false);
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
    @Override
    public CompactionTasks getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
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

    @Override
    public int getEstimatedRemainingTasks()
    {
        return getStrategies(false).stream()
                                   .flatMap(list -> list.stream())
                                   .mapToInt(CompactionStrategy::getEstimatedRemainingTasks)
                                   .sum();
    }

    @Override
    public int getTotalCompactions()
    {
        return getStrategies(false).stream()
                                   .flatMap(list -> list.stream())
                                   .mapToInt(CompactionStrategy::getTotalCompactions)
                                   .sum();
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public List<CompactionStrategy> getStrategies()
    {
        return getStrategies(true).stream().flatMap(List::stream).collect(Collectors.toList());
    }

    private List<List<CompactionStrategy>> getStrategies(boolean checkBoundaries)
    {
        if (checkBoundaries)
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

    @Override
    public List<CompactionStrategy> getStrategies(boolean isRepaired, @Nullable UUID pendingRepair)
    {
        readLock.lock();
        try
        {
            if (isRepaired)
                return Lists.newArrayList(repaired.allStrategies());
            else if (pendingRepair != null)
                return Lists.newArrayList(pendingRepairs.getStrategiesFor(pendingRepair));
            else
                return Lists.newArrayList(unrepaired.allStrategies());
        }
        finally
        {
            readLock.unlock();
        }
    }

    /**
     * @return the statistics for the compaction strategies that have compactions in progress or pending
     */
    @Override
    public List<CompactionStrategyStatistics> getStatistics()
    {
        return getStrategies(false).stream()
                                   .flatMap(list -> list.stream())
                                   .filter(strategy -> strategy.getTotalCompactions() > 0)
                                   .map(CompactionStrategy::getStatistics)
                                   .flatMap(List::stream)
                                   .collect(Collectors.toList());
    }

    private int getNumTokenPartitions()
    {
        return partitionSSTablesByTokenRange && currentBoundaries != null ? currentBoundaries.directories.size() : 1;
    }

    @Override
    public CompactionParams getCompactionParams()
    {
        return params;
    }

    @Override
    public CompactionParams getMetadataCompactionParams()
    {
        return metadataParams;
    }

    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       MetadataCollector collector,
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
                                                                                              collector,
                                                                                              header,
                                                                                              indexGroups,
                                                                                              lifecycleNewTracker);
        }
        finally
        {
            readLock.unlock();
        }
    }

    @Override
    public boolean supportsEarlyOpen()
    {
        return supportsEarlyOpen;
    }

    public ReentrantReadWriteLock.WriteLock getWriteLock()
    {
        return this.writeLock;
    }

    /**
     * This method is exposed for testing only
     * @return the LocalSession sessionIDs of any pending repairs
     */
    @VisibleForTesting
    public Set<UUID> pendingRepairs()
    {
        Set<UUID> ids = new HashSet<>();
        pendingRepairs.getManagers().forEach(p -> ids.addAll(p.getSessions()));
        return ids;
    }

    @Override
    public void repairSessionCompleted(UUID sessionID)
    {
        for (PendingRepairManager manager : pendingRepairs.getManagers())
            manager.removeSessionIfEmpty(sessionID);
    }

    //
    // CompactionObserver - because the strategies observe compactions, for CSM this is currently a no-op
    //

    @Override
    public void onInProgress(CompactionProgress progress)
    {

    }

    @Override
    public void onCompleted(UUID id)
    {

    }
}
