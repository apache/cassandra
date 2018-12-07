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
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Manages the compaction strategies.
 *
 * Currently has two instances of actual compaction strategies per data directory - one for repaired data and one for
 * unrepaired data. This is done to be able to totally separate the different sets of sstables.
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
    //TODO check possibility of getting rid of these locks by encapsulating these in an immutable atomic object
    private final List<AbstractCompactionStrategy> repaired = new ArrayList<>();
    private final List<AbstractCompactionStrategy> unrepaired = new ArrayList<>();
    private volatile CompactionParams params;
    private DiskBoundaries currentBoundaries;
    private volatile boolean enabled = true;
    private volatile boolean isActive = true;

    /**
        We keep a copy of the schema compaction parameters here to be able to decide if we
        should update the compaction strategy in {@link this#maybeReload(CFMetaData)} due to an ALTER.

        If a user changes the local compaction strategy and then later ALTERs a compaction parameter,
        we will use the new compaction parameters.
     **/
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
        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
        this.cfs = cfs;
        this.compactionLogger = new CompactionLogger(cfs, this);
        this.boundariesSupplier = boundariesSupplier;
        this.partitionSSTablesByTokenRange = partitionSSTablesByTokenRange;
        params = cfs.metadata.params.compaction;
        enabled = params.isEnabled();
        reload(cfs.metadata.params.compaction);
    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     *
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (!isEnabled())
                return null;

            List<AbstractCompactionStrategy> strategies = new ArrayList<>();

            strategies.addAll(repaired);
            strategies.addAll(unrepaired);
            Collections.sort(strategies, (o1, o2) -> Ints.compare(o2.getEstimatedRemainingTasks(), o1.getEstimatedRemainingTasks()));
            for (AbstractCompactionStrategy strategy : strategies)
            {
                AbstractCompactionTask task = strategy.getNextBackgroundTask(gcBefore);
                if (task != null)
                    return task;
            }
        }
        finally
        {
            readLock.unlock();
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
            repaired.forEach(AbstractCompactionStrategy::startup);
            unrepaired.forEach(AbstractCompactionStrategy::startup);
            shouldDefragment = repaired.get(0).shouldDefragment();
            supportsEarlyOpen = repaired.get(0).supportsEarlyOpen();
            fanout = (repaired.get(0) instanceof LeveledCompactionStrategy) ? ((LeveledCompactionStrategy) repaired.get(0)).getLevelFanoutSize() : LeveledCompactionStrategy.DEFAULT_LEVEL_FANOUT_SIZE;
        }
        finally
        {
            writeLock.unlock();
        }
        repaired.forEach(AbstractCompactionStrategy::startup);
        unrepaired.forEach(AbstractCompactionStrategy::startup);
        if (Stream.concat(repaired.stream(), unrepaired.stream()).anyMatch(cs -> cs.logAll))
            compactionLogger.enable();
    }

    /**
     * return the compaction strategy for the given sstable
     *
     * returns differently based on the repaired status and which vnode the compaction strategy belongs to
     * @param sstable
     * @return
     */
    protected AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        maybeReloadDiskBoundaries();
        return compactionStrategyFor(sstable);
    }

    @VisibleForTesting
    protected AbstractCompactionStrategy compactionStrategyFor(SSTableReader sstable)
    {
        // should not call maybeReloadDiskBoundaries because it may be called from within lock
        readLock.lock();
        try
        {
            int index = compactionStrategyIndexFor(sstable);
            if (sstable.isRepaired())
                return repaired.get(index);
            else
                return unrepaired.get(index);
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
    protected int compactionStrategyIndexFor(SSTableReader sstable)
    {
        // should not call maybeReload because it may be called from within lock
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

    public void shutdown()
    {
        writeLock.lock();
        try
        {
            isActive = false;
            repaired.forEach(AbstractCompactionStrategy::shutdown);
            unrepaired.forEach(AbstractCompactionStrategy::shutdown);
            compactionLogger.disable();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void maybeReload(CFMetaData metadata)
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
        schemaCompactionParams = cfs.metadata.params.compaction;

        if (disabledWithJMX || !shouldBeEnabled() && !enabledWithJMX)
            disable();
        else
            enable();
        startup();
    }

    public void replaceFlushed(Memtable memtable, Collection<SSTableReader> sstables)
    {
        cfs.getTracker().replaceFlushed(memtable, sstables);
        if (sstables != null && !sstables.isEmpty())
            CompactionManager.instance.submitBackground(cfs);
    }

    public int getUnleveledSSTables()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (repaired.get(0) instanceof LeveledCompactionStrategy && unrepaired.get(0) instanceof LeveledCompactionStrategy)
            {
                int count = 0;
                for (AbstractCompactionStrategy strategy : repaired)
                    count += ((LeveledCompactionStrategy) strategy).getLevelSize(0);
                for (AbstractCompactionStrategy strategy : unrepaired)
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
            if (repaired.get(0) instanceof LeveledCompactionStrategy && unrepaired.get(0) instanceof LeveledCompactionStrategy)
            {
                int[] res = new int[LeveledManifest.MAX_LEVEL_COUNT];
                for (AbstractCompactionStrategy strategy : repaired)
                {
                    int[] repairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                    res = sumArrays(res, repairedCountPerLevel);
                }
                for (AbstractCompactionStrategy strategy : unrepaired)
                {
                    int[] unrepairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                    res = sumArrays(res, unrepairedCountPerLevel);
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

    private static int[] sumArrays(int[] a, int[] b)
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

    public Directories getDirectories()
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            assert repaired.get(0).getClass().equals(unrepaired.get(0).getClass());
            return repaired.get(0).getDirectories();
        }
        finally
        {
            readLock.unlock();
        }
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

    private void handleListChangedNotification(Iterable<SSTableReader> added, Iterable<SSTableReader> removed)
    {
        // If reloaded, SSTables will be placed in their correct locations
        // so there is no need to process notification
        if (maybeReloadDiskBoundaries())
            return;

        readLock.lock();
        try
        {
            // a bit of gymnastics to be able to replace sstables in compaction strategies
            // we use this to know that a compaction finished and where to start the next compaction in LCS
            int locationSize = partitionSSTablesByTokenRange? currentBoundaries.directories.size() : 1;

            List<Set<SSTableReader>> repairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> repairedAdded = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedAdded = new ArrayList<>(locationSize);

            for (int i = 0; i < locationSize; i++)
            {
                repairedRemoved.add(new HashSet<>());
                repairedAdded.add(new HashSet<>());
                unrepairedRemoved.add(new HashSet<>());
                unrepairedAdded.add(new HashSet<>());
            }

            for (SSTableReader sstable : removed)
            {
                int i = compactionStrategyIndexFor(sstable);
                if (sstable.isRepaired())
                    repairedRemoved.get(i).add(sstable);
                else
                    unrepairedRemoved.get(i).add(sstable);
            }
            for (SSTableReader sstable : added)
            {
                int i = compactionStrategyIndexFor(sstable);
                if (sstable.isRepaired())
                    repairedAdded.get(i).add(sstable);
                else
                    unrepairedAdded.get(i).add(sstable);
            }
            for (int i = 0; i < locationSize; i++)
            {
                if (!repairedRemoved.get(i).isEmpty())
                    repaired.get(i).replaceSSTables(repairedRemoved.get(i), repairedAdded.get(i));
                else
                    repaired.get(i).addSSTables(repairedAdded.get(i));

                if (!unrepairedRemoved.get(i).isEmpty())
                    unrepaired.get(i).replaceSSTables(unrepairedRemoved.get(i), unrepairedAdded.get(i));
                else
                    unrepaired.get(i).addSSTables(unrepairedAdded.get(i));
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
            for (SSTableReader sstable : sstables)
            {
                int index = compactionStrategyIndexFor(sstable);
                if (sstable.isRepaired())
                {
                    unrepaired.get(index).removeSSTable(sstable);
                    repaired.get(index).addSSTable(sstable);
                }
                else
                {
                    repaired.get(index).removeSSTable(sstable);
                    unrepaired.get(index).addSSTable(sstable);
                }
            }
        }
        finally
        {
            readLock.unlock();
        }
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
    }

    public void enable()
    {
        writeLock.lock();
        try
        {
            if (repaired != null)
                repaired.forEach(AbstractCompactionStrategy::enable);
            if (unrepaired != null)
                unrepaired.forEach(AbstractCompactionStrategy::enable);
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
            // disable this first avoid asking disabled strategies for compaction tasks
            enabled = false;
            if (repaired != null)
                repaired.forEach(AbstractCompactionStrategy::disable);
            if (unrepaired != null)
                unrepaired.forEach(AbstractCompactionStrategy::disable);
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
    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            assert repaired.size() == unrepaired.size();
            List<Set<SSTableReader>> repairedSSTables = new ArrayList<>();
            List<Set<SSTableReader>> unrepairedSSTables = new ArrayList<>();

            for (int i = 0; i < repaired.size(); i++)
            {
                repairedSSTables.add(new HashSet<>());
                unrepairedSSTables.add(new HashSet<>());
            }

            for (SSTableReader sstable : sstables)
            {
                if (sstable.isRepaired())
                    repairedSSTables.get(compactionStrategyIndexFor(sstable)).add(sstable);
                else
                    unrepairedSSTables.get(compactionStrategyIndexFor(sstable)).add(sstable);
            }

            List<ISSTableScanner> scanners = new ArrayList<>(sstables.size());
            for (int i = 0; i < repairedSSTables.size(); i++)
            {
                if (!repairedSSTables.get(i).isEmpty())
                    scanners.addAll(repaired.get(i).getScanners(repairedSSTables.get(i), ranges).scanners);
            }
            for (int i = 0; i < unrepairedSSTables.size(); i++)
            {
                if (!unrepairedSSTables.get(i).isEmpty())
                    scanners.addAll(unrepaired.get(i).getScanners(unrepairedSSTables.get(i), ranges).scanners);
            }

            return new AbstractCompactionStrategy.ScannerList(scanners);
        }
        finally
        {
            readLock.unlock();
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
            Map<Integer, List<SSTableReader>> groups = sstablesToGroup.stream().collect(Collectors.groupingBy((s) -> compactionStrategyIndexFor(s)));
            Collection<Collection<SSTableReader>> anticompactionGroups = new ArrayList<>();

            for (Map.Entry<Integer, List<SSTableReader>> group : groups.entrySet())
                anticompactionGroups.addAll(unrepaired.get(group.getKey()).groupSSTablesForAntiCompaction(group.getValue()));
            return anticompactionGroups;
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
            return unrepaired.get(0).getMaxSSTableBytes();
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
            for (SSTableReader sstable : input)
            {
                if (sstable.isRepaired() != repaired)
                    throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
                if (firstIndex != compactionStrategyIndexFor(sstable))
                    throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
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
                    for (AbstractCompactionStrategy strategy : repaired)
                    {
                        Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                        if (task != null)
                            tasks.addAll(task);
                    }
                    for (AbstractCompactionStrategy strategy : unrepaired)
                    {
                        Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                        if (task != null)
                            tasks.addAll(task);
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
        return getUserDefinedTasks(sstables, gcBefore, false);
    }

    public List<AbstractCompactionTask> getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore, boolean validateForCompaction)
    {
        maybeReloadDiskBoundaries();
        List<AbstractCompactionTask> ret = new ArrayList<>();
        readLock.lock();
        try
        {
            if (validateForCompaction)
                validateForCompaction(sstables);

            Map<Integer, List<SSTableReader>> repairedSSTables = sstables.stream()
                                                                         .filter(s -> !s.isMarkedSuspect() && s.isRepaired())
                                                                         .collect(Collectors.groupingBy((s) -> compactionStrategyIndexFor(s)));

            Map<Integer, List<SSTableReader>> unrepairedSSTables = sstables.stream()
                                                                           .filter(s -> !s.isMarkedSuspect() && !s.isRepaired())
                                                                           .collect(Collectors.groupingBy((s) -> compactionStrategyIndexFor(s)));


            for (Map.Entry<Integer, List<SSTableReader>> group : repairedSSTables.entrySet())
                ret.add(repaired.get(group.getKey()).getUserDefinedTask(group.getValue(), gcBefore));

            for (Map.Entry<Integer, List<SSTableReader>> group : unrepairedSSTables.entrySet())
                ret.add(unrepaired.get(group.getKey()).getUserDefinedTask(group.getValue(), gcBefore));

            return ret;
        }
        finally
        {
            readLock.unlock();
        }
    }

    /**
     * @deprecated use {@link #getUserDefinedTasks(Collection, int)} instead.
     */
    @Deprecated()
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        List<AbstractCompactionTask> tasks = getUserDefinedTasks(sstables, gcBefore, true);
        assert tasks.size() == 1;
        return tasks.get(0);
    }

    public int getEstimatedRemainingTasks()
    {
        maybeReloadDiskBoundaries();
        int tasks = 0;
        readLock.lock();
        try
        {

            for (AbstractCompactionStrategy strategy : repaired)
                tasks += strategy.getEstimatedRemainingTasks();
            for (AbstractCompactionStrategy strategy : unrepaired)
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
            return unrepaired.get(0).getName();
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
            return Arrays.asList(repaired, unrepaired);
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

    private void setStrategy(CompactionParams params)
    {
        repaired.forEach(AbstractCompactionStrategy::shutdown);
        unrepaired.forEach(AbstractCompactionStrategy::shutdown);
        repaired.clear();
        unrepaired.clear();

        if (partitionSSTablesByTokenRange)
        {
            for (int i = 0; i < currentBoundaries.directories.size(); i++)
            {
                repaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
                unrepaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
            }
        }
        else
        {
            repaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
            unrepaired.add(CFMetaData.createCompactionStrategyInstance(cfs, params));
        }
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
                                                       MetadataCollector collector,
                                                       SerializationHeader header,
                                                       Collection<Index> indexes,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        maybeReloadDiskBoundaries();
        readLock.lock();
        try
        {
            if (repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
            {
                return unrepaired.get(0).createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, indexes, lifecycleNewTracker);
            }
            else
            {
                return repaired.get(0).createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, indexes, lifecycleNewTracker);
            }
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean isRepaired(AbstractCompactionStrategy strategy)
    {
        readLock.lock();
        try
        {
            return repaired.contains(strategy);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public List<String> getStrategyFolders(AbstractCompactionStrategy strategy)
    {
        readLock.lock();
        try
        {
            List<Directories.DataDirectory> locations = currentBoundaries.directories;
            if (partitionSSTablesByTokenRange)
            {
                int unrepairedIndex = unrepaired.indexOf(strategy);
                if (unrepairedIndex > 0)
                {
                    return Collections.singletonList(locations.get(unrepairedIndex).location.getAbsolutePath());
                }
                int repairedIndex = repaired.indexOf(strategy);
                if (repairedIndex > 0)
                {
                    return Collections.singletonList(locations.get(repairedIndex).location.getAbsolutePath());
                }
            }
            List<String> folders = new ArrayList<>(locations.size());
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
}
