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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.index.Index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;

/**
 * Manages the compaction strategies.
 *
 * For each directory, a separate compaction strategy instance for both repaired and unrepaired data, and also one instance
 * for each pending repair. This is done to keep the different sets of sstables completely separate.
 */

public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    public final CompactionLogger compactionLogger;
    private final ColumnFamilyStore cfs;
    private final List<AbstractCompactionStrategy> repaired = new ArrayList<>();
    private final List<AbstractCompactionStrategy> unrepaired = new ArrayList<>();
    private final List<PendingRepairManager> pendingRepairs = new ArrayList<>();
    private volatile boolean enabled = true;
    private volatile boolean isActive = true;
    private volatile CompactionParams params;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    /*
        We keep a copy of the schema compaction parameters here to be able to decide if we
        should update the compaction strategy in maybeReloadCompactionStrategy() due to an ALTER.

        If a user changes the local compaction strategy and then later ALTERs a compaction parameter,
        we will use the new compaction parameters.
     */
    private volatile CompactionParams schemaCompactionParams;
    private Directories.DataDirectory[] locations;
    private boolean shouldDefragment;
    private int fanout;


    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
        this.cfs = cfs;
        this.compactionLogger = new CompactionLogger(cfs, this);
        reload(cfs.metadata());
        params = cfs.metadata().params.compaction;
        locations = getDirectories().getWriteableLocations();
        enabled = params.isEnabled();

    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     *
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        readLock.lock();
        try
        {
            if (!isEnabled())
                return null;

            maybeReload(cfs.metadata());

            // first try to promote/demote sstables from completed repairs
            ArrayList<Pair<Integer, PendingRepairManager>> pendingRepairManagers = new ArrayList<>(pendingRepairs.size());
            for (PendingRepairManager pendingRepair : pendingRepairs)
            {
                int numPending = pendingRepair.getNumPendingRepairFinishedTasks();
                if (numPending > 0)
                {
                    pendingRepairManagers.add(Pair.create(numPending, pendingRepair));
                }
            }
            if (!pendingRepairManagers.isEmpty())
            {
                pendingRepairManagers.sort((x, y) -> y.left - x.left);
                for (Pair<Integer, PendingRepairManager> pair : pendingRepairManagers)
                {
                    AbstractCompactionTask task = pair.right.getNextRepairFinishedTask();
                    if (task != null)
                    {
                        return task;
                    }
                }
            }

            // sort compaction task suppliers by remaining tasks descending
            ArrayList<Pair<Integer, Supplier<AbstractCompactionTask>>> sortedSuppliers = new ArrayList<>(repaired.size() + unrepaired.size() + 1);

            for (AbstractCompactionStrategy strategy : repaired)
                sortedSuppliers.add(Pair.create(strategy.getEstimatedRemainingTasks(), () -> strategy.getNextBackgroundTask(gcBefore)));

            for (AbstractCompactionStrategy strategy : unrepaired)
                sortedSuppliers.add(Pair.create(strategy.getEstimatedRemainingTasks(), () -> strategy.getNextBackgroundTask(gcBefore)));

            for (PendingRepairManager pending : pendingRepairs)
                sortedSuppliers.add(Pair.create(pending.getMaxEstimatedRemainingTasks(), () -> pending.getNextBackgroundTask(gcBefore)));

            sortedSuppliers.sort((x, y) -> y.left - x.left);

            // return the first non-null task
            AbstractCompactionTask task;
            Iterator<Supplier<AbstractCompactionTask>> suppliers = Iterables.transform(sortedSuppliers, p -> p.right).iterator();
            assert suppliers.hasNext();

            do
            {
                task = suppliers.next().get();
            }
            while (suppliers.hasNext() && task == null);

            return task;
        }
        finally
        {
            readLock.unlock();
        }
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
                    getCompactionStrategyFor(sstable).addSSTable(sstable);
            }
            repaired.forEach(AbstractCompactionStrategy::startup);
            unrepaired.forEach(AbstractCompactionStrategy::startup);
            pendingRepairs.forEach(PendingRepairManager::startup);
            shouldDefragment = repaired.get(0).shouldDefragment();
            fanout = (repaired.get(0) instanceof LeveledCompactionStrategy) ? ((LeveledCompactionStrategy) repaired.get(0)).getLevelFanoutSize() : LeveledCompactionStrategy.DEFAULT_LEVEL_FANOUT_SIZE;
        }
        finally
        {
            writeLock.unlock();
        }
        repaired.forEach(AbstractCompactionStrategy::startup);
        unrepaired.forEach(AbstractCompactionStrategy::startup);
        pendingRepairs.forEach(PendingRepairManager::startup);
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
    public AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        int index = getCompactionStrategyIndex(cfs, sstable);
        readLock.lock();
        try
        {
            if (sstable.isPendingRepair())
                return pendingRepairs.get(index).getOrCreate(sstable);
            else if (sstable.isRepaired())
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
     * @param cfs
     * @param sstable
     * @return
     */
    public static int getCompactionStrategyIndex(ColumnFamilyStore cfs, SSTableReader sstable)
    {
        if (!cfs.getPartitioner().splitter().isPresent())
            return 0;

        DiskBoundaries boundaries = cfs.getDiskBoundaries();
        List<Directories.DataDirectory> directories = boundaries.directories;
        if (boundaries.positions == null)
            return getCompactionStrategyIndex(directories, sstable.descriptor);

        int pos = Collections.binarySearch(boundaries.positions, sstable.first);
        assert pos < 0; // boundaries are .minkeybound and .maxkeybound so they should never be equal
        return -pos - 1;
    }

    /**
     * get the index for the descriptor based on the existing directories
     * @param locations
     * @param descriptor
     * @return
     */
    private static int getCompactionStrategyIndex(List<Directories.DataDirectory> directories, Descriptor descriptor)
    {
         // try to figure out location based on sstable directory:
         for (int i = 0; i < directories.size(); i++)
         {
             Directories.DataDirectory directory = directories.get(i);
             if (descriptor.directory.getAbsolutePath().startsWith(directory.location.getAbsolutePath()))
                 return i;
         }
         return 0;
    }

    @VisibleForTesting
    List<AbstractCompactionStrategy> getRepaired()
    {
        return repaired;
    }

    @VisibleForTesting
    List<AbstractCompactionStrategy> getUnrepaired()
    {
        return unrepaired;
    }

    @VisibleForTesting
    List<AbstractCompactionStrategy> getForPendingRepair(UUID sessionID)
    {
        List<AbstractCompactionStrategy> strategies = new ArrayList<>(pendingRepairs.size());
        pendingRepairs.forEach(p -> strategies.add(p.get(sessionID)));
        return strategies;
    }

    @VisibleForTesting
    Set<UUID> pendingRepairs()
    {
        Set<UUID> ids = new HashSet<>();
        pendingRepairs.forEach(p -> ids.addAll(p.getSessions()));
        return ids;
    }

    public boolean hasDataForPendingRepair(UUID sessionID)
    {
        return Iterables.any(pendingRepairs, prm -> prm.hasDataForSession(sessionID));
    }

    public void shutdown()
    {
        writeLock.lock();
        try
        {
            isActive = false;
            repaired.forEach(AbstractCompactionStrategy::shutdown);
            unrepaired.forEach(AbstractCompactionStrategy::shutdown);
            pendingRepairs.forEach(PendingRepairManager::shutdown);
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
        if (metadata.params.compaction.equals(schemaCompactionParams) &&
            Arrays.equals(locations, cfs.getDirectories().getWriteableLocations())) // any drives broken?
            return;

        writeLock.lock();
        try
        {
            reload(metadata);
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
     * @param metadata
     */
    private void reload(TableMetadata metadata)
    {
        boolean disabledWithJMX = !enabled && shouldBeEnabled();
        if (!metadata.params.compaction.equals(schemaCompactionParams))
            logger.trace("Recreating compaction strategy - compaction parameters changed for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
        else if (!Arrays.equals(locations, cfs.getDirectories().getWriteableLocations()))
            logger.trace("Recreating compaction strategy - writeable locations changed for {}.{}", cfs.keyspace.getName(), cfs.getTableName());

        setStrategy(metadata.params.compaction);
        schemaCompactionParams = metadata.params.compaction;

        if (disabledWithJMX || !shouldBeEnabled())
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
                for (PendingRepairManager pendingManager : pendingRepairs)
                    for (AbstractCompactionStrategy strategy : pendingManager.getStrategies())
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
                for (PendingRepairManager pending : pendingRepairs)
                {
                    int[] pendingRepairCountPerLevel = pending.getSSTableCountPerLevel();
                    res = sumArrays(res, pendingRepairCountPerLevel);
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

    public Directories getDirectories()
    {
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
        readLock.lock();
        try
        {
            for (SSTableReader sstable : added)
                getCompactionStrategyFor(sstable).addSSTable(sstable);
        }
        finally
        {
            readLock.unlock();
        }
    }

    private void handleListChangedNotification(Iterable<SSTableReader> added, Iterable<SSTableReader> removed)
    {
        // a bit of gymnastics to be able to replace sstables in compaction strategies
        // we use this to know that a compaction finished and where to start the next compaction in LCS
        Directories.DataDirectory [] locations = cfs.getDirectories().getWriteableLocations();
        int locationSize = cfs.getPartitioner().splitter().isPresent() ? locations.length : 1;

        List<Set<SSTableReader>> pendingRemoved = new ArrayList<>(locationSize);
        List<Set<SSTableReader>> pendingAdded = new ArrayList<>(locationSize);
        List<Set<SSTableReader>> repairedRemoved = new ArrayList<>(locationSize);
        List<Set<SSTableReader>> repairedAdded = new ArrayList<>(locationSize);
        List<Set<SSTableReader>> unrepairedRemoved = new ArrayList<>(locationSize);
        List<Set<SSTableReader>> unrepairedAdded = new ArrayList<>(locationSize);

        for (int i = 0; i < locationSize; i++)
        {
            pendingRemoved.add(new HashSet<>());
            pendingAdded.add(new HashSet<>());
            repairedRemoved.add(new HashSet<>());
            repairedAdded.add(new HashSet<>());
            unrepairedRemoved.add(new HashSet<>());
            unrepairedAdded.add(new HashSet<>());
        }

        for (SSTableReader sstable : removed)
        {
            int i = getCompactionStrategyIndex(cfs, sstable);
            if (sstable.isPendingRepair())
                pendingRemoved.get(i).add(sstable);
            else if (sstable.isRepaired())
                repairedRemoved.get(i).add(sstable);
            else
                unrepairedRemoved.get(i).add(sstable);
        }
        for (SSTableReader sstable : added)
        {
            int i = getCompactionStrategyIndex(cfs, sstable);
            if (sstable.isPendingRepair())
                pendingAdded.get(i).add(sstable);
            else if (sstable.isRepaired())
                repairedAdded.get(i).add(sstable);
            else
                unrepairedAdded.get(i).add(sstable);
        }
        // we need write lock here since we might be moving sstables between strategies
        writeLock.lock();
        try
        {
            for (int i = 0; i < locationSize; i++)
            {

                if (!pendingRemoved.get(i).isEmpty())
                {
                    pendingRepairs.get(i).replaceSSTables(pendingRemoved.get(i), pendingAdded.get(i));
                }
                else
                {
                    PendingRepairManager pendingManager = pendingRepairs.get(i);
                    pendingAdded.get(i).forEach(s -> pendingManager.addSSTable(s));
                }

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
            writeLock.unlock();
        }
    }

    private void handleRepairStatusChangedNotification(Iterable<SSTableReader> sstables)
    {
        // we need a write lock here since we move sstables from one strategy instance to another
        writeLock.lock();
        try
        {
            for (SSTableReader sstable : sstables)
            {
                int index = getCompactionStrategyIndex(cfs, sstable);
                if (sstable.isPendingRepair())
                {
                    pendingRepairs.get(index).addSSTable(sstable);
                    unrepaired.get(index).removeSSTable(sstable);
                    repaired.get(index).removeSSTable(sstable);
                }
                else if (sstable.isRepaired())
                {
                    pendingRepairs.get(index).removeSSTable(sstable);
                    unrepaired.get(index).removeSSTable(sstable);
                    repaired.get(index).addSSTable(sstable);
                }
                else
                {
                    pendingRepairs.get(index).removeSSTable(sstable);
                    repaired.get(index).removeSSTable(sstable);
                    unrepaired.get(index).addSSTable(sstable);
                }
            }
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private void handleDeletingNotification(SSTableReader deleted)
    {
        writeLock.lock();
        try
        {
            getCompactionStrategyFor(deleted).removeSSTable(deleted);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void handleNotification(INotification notification, Object sender)
    {
        maybeReload(cfs.metadata());
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
            if (pendingRepairs != null)
                pendingRepairs.forEach(PendingRepairManager::enable);
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
            if (pendingRepairs != null)
                pendingRepairs.forEach(PendingRepairManager::disable);
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
        assert repaired.size() == unrepaired.size();
        assert repaired.size() == pendingRepairs.size();

        int numRepaired = repaired.size();
        List<Set<SSTableReader>> pendingSSTables = new ArrayList<>(numRepaired);
        List<Set<SSTableReader>> repairedSSTables = new ArrayList<>(numRepaired);
        List<Set<SSTableReader>> unrepairedSSTables = new ArrayList<>(numRepaired);

        for (int i = 0; i < numRepaired; i++)
        {
            pendingSSTables.add(new HashSet<>());
            repairedSSTables.add(new HashSet<>());
            unrepairedSSTables.add(new HashSet<>());
        }

        List<ISSTableScanner> scanners = new ArrayList<>(sstables.size());

        readLock.lock();
        try
        {
            for (SSTableReader sstable : sstables)
            {
                int idx = getCompactionStrategyIndex(cfs, sstable);
                if (sstable.isPendingRepair())
                    pendingSSTables.get(idx).add(sstable);
                else if (sstable.isRepaired())
                    repairedSSTables.get(idx).add(sstable);
                else
                    unrepairedSSTables.get(idx).add(sstable);
            }

            for (int i = 0; i < pendingSSTables.size(); i++)
            {
                if (!pendingSSTables.get(i).isEmpty())
                    scanners.addAll(pendingRepairs.get(i).getScanners(pendingSSTables.get(i), ranges));
            }
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
        readLock.lock();
        try
        {
            Map<Integer, List<SSTableReader>> groups = sstablesToGroup.stream().collect(Collectors.groupingBy((s) -> getCompactionStrategyIndex(cfs, s)));
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
        maybeReload(cfs.metadata());
        validateForCompaction(txn.originals(), cfs, getDirectories());
        return getCompactionStrategyFor(txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    private static void validateForCompaction(Iterable<SSTableReader> input, ColumnFamilyStore cfs, Directories directories)
    {
        SSTableReader firstSSTable = Iterables.getFirst(input, null);
        assert firstSSTable != null;
        boolean repaired = firstSSTable.isRepaired();
        int firstIndex = getCompactionStrategyIndex(cfs, firstSSTable);
        boolean isPending = firstSSTable.isPendingRepair();
        UUID pendingRepair = firstSSTable.getSSTableMetadata().pendingRepair;
        for (SSTableReader sstable : input)
        {
            if (sstable.isRepaired() != repaired)
                throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
            if (firstIndex != getCompactionStrategyIndex(cfs, sstable))
                throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
            if (isPending && !pendingRepair.equals(sstable.getSSTableMetadata().pendingRepair))
                throw new UnsupportedOperationException("You can't compact sstables from different pending repair sessions");
        }
    }

    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput)
    {
        maybeReload(cfs.metadata());
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

                    for (PendingRepairManager pending : pendingRepairs)
                    {
                        Collection<AbstractCompactionTask> pendingRepairTasks = pending.getMaximalTasks(gcBefore, splitOutput);
                        if (pendingRepairTasks != null)
                            tasks.addAll(pendingRepairTasks);
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
        maybeReload(cfs.metadata());
        List<AbstractCompactionTask> ret = new ArrayList<>();
        readLock.lock();
        try
        {
            Map<Integer, List<SSTableReader>> repairedSSTables = sstables.stream()
                                                                         .filter(s -> !s.isMarkedSuspect() && s.isRepaired() && !s.isPendingRepair())
                                                                         .collect(Collectors.groupingBy((s) -> getCompactionStrategyIndex(cfs, s)));

            Map<Integer, List<SSTableReader>> unrepairedSSTables = sstables.stream()
                                                                           .filter(s -> !s.isMarkedSuspect() && !s.isRepaired() && !s.isPendingRepair())
                                                                           .collect(Collectors.groupingBy((s) -> getCompactionStrategyIndex(cfs, s)));

            Map<Integer, List<SSTableReader>> pendingSSTables = sstables.stream()
                                                                        .filter(s -> !s.isMarkedSuspect() && s.isPendingRepair())
                                                                        .collect(Collectors.groupingBy((s) -> getCompactionStrategyIndex(cfs, s)));

            for (Map.Entry<Integer, List<SSTableReader>> group : repairedSSTables.entrySet())
                ret.add(repaired.get(group.getKey()).getUserDefinedTask(group.getValue(), gcBefore));

            for (Map.Entry<Integer, List<SSTableReader>> group : unrepairedSSTables.entrySet())
                ret.add(unrepaired.get(group.getKey()).getUserDefinedTask(group.getValue(), gcBefore));

            for (Map.Entry<Integer, List<SSTableReader>> group : pendingSSTables.entrySet())
                ret.addAll(pendingRepairs.get(group.getKey()).createUserDefinedTasks(group.getValue(), gcBefore));

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
        validateForCompaction(sstables, cfs, getDirectories());
        List<AbstractCompactionTask> tasks = getUserDefinedTasks(sstables, gcBefore);
        assert tasks.size() == 1;
        return tasks.get(0);
    }

    public int getEstimatedRemainingTasks()
    {
        int tasks = 0;
        readLock.lock();
        try
        {
            for (AbstractCompactionStrategy strategy : repaired)
                tasks += strategy.getEstimatedRemainingTasks();
            for (AbstractCompactionStrategy strategy : unrepaired)
                tasks += strategy.getEstimatedRemainingTasks();
            for (PendingRepairManager pending : pendingRepairs)
                tasks += pending.getEstimatedRemainingTasks();
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
        readLock.lock();
        try
        {
            List<AbstractCompactionStrategy> pending = new ArrayList<>();
            pendingRepairs.forEach(p -> pending.addAll(p.getStrategies()));
            return Arrays.asList(repaired, unrepaired, pending);
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
        pendingRepairs.forEach(PendingRepairManager::shutdown);
        repaired.clear();
        unrepaired.clear();
        pendingRepairs.clear();

        if (cfs.getPartitioner().splitter().isPresent())
        {
            locations = cfs.getDirectories().getWriteableLocations();
            for (int i = 0; i < locations.length; i++)
            {
                repaired.add(cfs.createCompactionStrategyInstance(params));
                unrepaired.add(cfs.createCompactionStrategyInstance(params));
                pendingRepairs.add(new PendingRepairManager(cfs, params));
            }
        }
        else
        {
            repaired.add(cfs.createCompactionStrategyInstance(params));
            unrepaired.add(cfs.createCompactionStrategyInstance(params));
            pendingRepairs.add(new PendingRepairManager(cfs, params));
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
                                                       UUID pendingRepair,
                                                       MetadataCollector collector,
                                                       SerializationHeader header,
                                                       Collection<Index> indexes,
                                                       LifecycleTransaction txn)
    {
        readLock.lock();
        try
        {
            // to avoid creating a compaction strategy for the wrong pending repair manager, we get the index based on where the sstable is to be written
            int index = cfs.getPartitioner().splitter().isPresent()
                        ? getCompactionStrategyIndex(Arrays.asList(getDirectories().getWriteableLocations()), descriptor)
                        : 0;
            if (pendingRepair != ActiveRepairService.NO_PENDING_REPAIR)
                return pendingRepairs.get(index).getOrCreate(pendingRepair).createSSTableMultiWriter(descriptor, keyCount, ActiveRepairService.UNREPAIRED_SSTABLE, pendingRepair, collector, header, indexes, txn);
            else if (repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
                return unrepaired.get(index).createSSTableMultiWriter(descriptor, keyCount, repairedAt, ActiveRepairService.NO_PENDING_REPAIR, collector, header, indexes, txn);
            else
                return repaired.get(index).createSSTableMultiWriter(descriptor, keyCount, repairedAt, ActiveRepairService.NO_PENDING_REPAIR, collector, header, indexes, txn);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean isRepaired(AbstractCompactionStrategy strategy)
    {
        return repaired.contains(strategy);
    }

    public List<String> getStrategyFolders(AbstractCompactionStrategy strategy)
    {
        Directories.DataDirectory[] locations = cfs.getDirectories().getWriteableLocations();
        if (cfs.getPartitioner().splitter().isPresent())
        {
            int unrepairedIndex = unrepaired.indexOf(strategy);
            if (unrepairedIndex > 0)
            {
                return Collections.singletonList(locations[unrepairedIndex].location.getAbsolutePath());
            }
            int repairedIndex = repaired.indexOf(strategy);
            if (repairedIndex > 0)
            {
                return Collections.singletonList(locations[repairedIndex].location.getAbsolutePath());
            }
            for (int i = 0; i < pendingRepairs.size(); i++)
            {
                PendingRepairManager pending = pendingRepairs.get(i);
                if (pending.hasStrategy(strategy))
                {
                    return Collections.singletonList(locations[i].location.getAbsolutePath());
                }
            }
        }
        List<String> folders = new ArrayList<>(locations.length);
        for (Directories.DataDirectory location : locations)
        {
            folders.add(location.location.getAbsolutePath());
        }
        return folders;
    }

    public boolean supportsEarlyOpen()
    {
        return repaired.get(0).supportsEarlyOpen();
    }

    @VisibleForTesting
    List<PendingRepairManager> getPendingRepairManagers()
    {
        return pendingRepairs;
    }

    /**
     * Mutates sstable repairedAt times and notifies listeners of the change with the writeLock held. Prevents races
     * with other processes between when the metadata is changed and when sstables are moved between strategies.
     */
    public void mutateRepaired(Collection<SSTableReader> sstables, long repairedAt, UUID pendingRepair) throws IOException
    {
        Set<SSTableReader> changed = new HashSet<>();

        writeLock.lock();
        try
        {
            for (SSTableReader sstable: sstables)
            {
                sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, repairedAt, pendingRepair);
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
