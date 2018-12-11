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


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.AbstractStrategyHolder.TaskSupplier;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableMetadataChanged;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
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
    private volatile boolean enabled = true;
    private volatile boolean isActive = true;

    /*
        We keep a copy of the schema compaction parameters here to be able to decide if we
        should update the compaction strategy in maybeReload() due to an ALTER.

        If a user changes the local compaction strategy and then later ALTERs a compaction parameter,
        we will use the new compaction parameters.
     */
    private volatile CompactionParams schemaCompactionParams;
    private boolean shouldDefragment;
    private boolean supportsEarlyOpen;
    private int fanout;

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
        params = cfs.metadata().params.compaction;
        enabled = params.isEnabled();
        reload(cfs.metadata().params.compaction);
    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
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
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    AbstractCompactionTask findUpgradeSSTableTask()
    {
        if (!isEnabled() || !DatabaseDescriptor.automaticSSTableUpgrade())
            return null;
        Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
        List<SSTableReader> potentialUpgrade = cfs.getLiveSSTables()
                                                  .stream()
                                                  .filter(s -> !compacting.contains(s) && !s.descriptor.version.isLatestVersion())
                                                  .sorted((o1, o2) -> {
                                                      File f1 = new File(o1.descriptor.filenameFor(Component.DATA));
                                                      File f2 = new File(o2.descriptor.filenameFor(Component.DATA));
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
            shouldDefragment = repaired.first().shouldDefragment();
            supportsEarlyOpen = repaired.first().supportsEarlyOpen();
            fanout = (repaired.first() instanceof LeveledCompactionStrategy) ? ((LeveledCompactionStrategy) repaired.first()).getLevelFanoutSize() : LeveledCompactionStrategy.DEFAULT_LEVEL_FANOUT_SIZE;
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
    @VisibleForTesting
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

    public boolean hasDataForPendingRepair(UUID sessionID)
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

    public void maybeReload(TableMetadata metadata)
    {
        // compare the old schema configuration to the new one, ignore any locally set changes.
        if (metadata.params.compaction.equals(schemaCompactionParams))
            return;

        writeLock.lock();
        try
        {
            // compare the old schema configuration to the new one, ignore any locally set changes.
            if (metadata.params.compaction.equals(schemaCompactionParams))
                return;
            reload(metadata.params.compaction);
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
    protected boolean maybeReloadDiskBoundaries()
    {
        if (!currentBoundaries.isOutOfDate())
            return false;

        writeLock.lock();
        try
        {
            if (!currentBoundaries.isOutOfDate())
                return false;
            reload(params);
            return true;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Reload the compaction strategies
     *
     * Called after changing configuration and at startup.
     * @param newCompactionParams
     */
    private void reload(CompactionParams newCompactionParams)
    {
        boolean enabledWithJMX = enabled && !shouldBeEnabled();
        boolean disabledWithJMX = !enabled && shouldBeEnabled();

        if (currentBoundaries != null)
        {
            if (!newCompactionParams.equals(schemaCompactionParams))
                logger.debug("Recreating compaction strategy - compaction parameters changed for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
            else if (currentBoundaries.isOutOfDate())
                logger.debug("Recreating compaction strategy - disk boundaries are out of date for {}.{}.", cfs.keyspace.getName(), cfs.getTableName());
        }

        if (currentBoundaries == null || currentBoundaries.isOutOfDate())
            currentBoundaries = boundariesSupplier.get();

        setStrategy(newCompactionParams);
        schemaCompactionParams = cfs.metadata().params.compaction;

        if (disabledWithJMX || !shouldBeEnabled() && !enabledWithJMX)
            disable();
        else
            enable();
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
                int[] res = new int[LeveledManifest.MAX_LEVEL_COUNT];
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

    public boolean shouldDefragment()
    {
        return shouldDefragment;
    }

    private void handleFlushNotification(Iterable<SSTableReader> added)
    {
        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReloadDiskBoundaries())
            return;

        readLock.lock();
        try
        {
            for (SSTableReader sstable : added)
                compactionStrategyFor(sstable).addSSTable(sstable);
        }
        finally
        {
            readLock.unlock();
        }
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
    @VisibleForTesting
    List<GroupedSSTableContainer> groupSSTables(Iterable<SSTableReader> sstables)
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

    private void handleListChangedNotification(Iterable<SSTableReader> added, Iterable<SSTableReader> removed)
    {
        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReloadDiskBoundaries())
            return;

        readLock.lock();
        try
        {
            List<GroupedSSTableContainer> addedGroups = groupSSTables(added);
            List<GroupedSSTableContainer> removedGroups = groupSSTables(removed);
            for (int i=0; i<holders.size(); i++)
            {
                holders.get(i).replaceSSTables(removedGroups.get(i), addedGroups.get(i));
            }
        }
        finally
        {
            readLock.unlock();
        }
    }

    private void handleRepairStatusChangedNotification(Iterable<SSTableReader> sstables)
    {
        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReloadDiskBoundaries())
            return;
        // we need a write lock here since we move sstables from one strategy instance to another
        readLock.lock();
        try
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
        finally
        {
            readLock.unlock();
        }
    }

    private void handleMetadataChangedNotification(SSTableReader sstable, StatsMetadata oldMetadata)
    {
        AbstractCompactionStrategy acs = getCompactionStrategyFor(sstable);
        acs.metadataChanged(oldMetadata, sstable);
    }

    private void handleDeletingNotification(SSTableReader deleted)
    {
        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReloadDiskBoundaries())
            return;
        readLock.lock();
        try
        {
            compactionStrategyFor(deleted).removeSSTable(deleted);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            handleFlushNotification(((SSTableAddedNotification) notification).added);
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
    @SuppressWarnings("resource")
    public AbstractCompactionStrategy.ScannerList maybeGetScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        List<ISSTableScanner> scanners = new ArrayList<>(sstables.size());
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
        readLock.lock();
        try
        {
            return unrepaired.first().getMaxSSTableBytes();
        }
        finally
        {
            readLock.unlock();
        }
    }

    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
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

    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput)
    {
        maybeReloadDiskBoundaries();
        // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired strategies mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>()
        {
            @Override
            public Collection<AbstractCompactionTask> call()
            {
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
                if (tasks.isEmpty())
                    return null;
                return tasks;
            }
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
    public List<AbstractCompactionTask> getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
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
            return ret;
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

    public boolean shouldBeEnabled()
    {
        return params.isEnabled();
    }

    public String getName()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            return unrepaired.first().getName();
        }
        finally
        {
            readLock.unlock();
        }
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

    public void setNewLocalCompactionStrategy(CompactionParams params)
    {
        logger.info("Switching local compaction strategy from {} to {}}", this.params, params);
        writeLock.lock();
        try
        {
            setStrategy(params);
            if (shouldBeEnabled())
                enable();
            else
                disable();
            startup();
        }
        finally
        {
            writeLock.unlock();
        }
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
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       MetadataCollector collector,
                                                       SerializationHeader header,
                                                       Collection<Index> indexes,
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
                                                                                              indexes,
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
                        return Collections.singletonList(locations[idx].location.getAbsolutePath());
                }
            }
            List<String> folders = new ArrayList<>(locations.length);
            for (Directories.DataDirectory location : locations)
            {
                folders.add(location.location.getAbsolutePath());
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
    public void mutateRepaired(Collection<SSTableReader> sstables, long repairedAt, UUID pendingRepair, boolean isTransient) throws IOException
    {
        Set<SSTableReader> changed = new HashSet<>();

        writeLock.lock();
        try
        {
            for (SSTableReader sstable: sstables)
            {
                sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, repairedAt, pendingRepair, isTransient);
                sstable.reloadSSTableMetadata();
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
}
