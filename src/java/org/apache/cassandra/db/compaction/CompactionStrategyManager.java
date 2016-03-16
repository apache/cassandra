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
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.apache.cassandra.index.Index;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.PartitionPosition;
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
import org.apache.cassandra.service.StorageService;

/**
 * Manages the compaction strategies.
 *
 * Currently has two instances of actual compaction strategies per data directory - one for repaired data and one for
 * unrepaired data. This is done to be able to totally separate the different sets of sstables.
 */

public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    private final ColumnFamilyStore cfs;
    private final List<AbstractCompactionStrategy> repaired = new ArrayList<>();
    private final List<AbstractCompactionStrategy> unrepaired = new ArrayList<>();
    private volatile boolean enabled = true;
    public volatile boolean isActive = true;
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

    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
        this.cfs = cfs;
        reload(cfs.metadata);
        params = cfs.metadata.params.compaction;
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
        if (!isEnabled())
            return null;

        maybeReload(cfs.metadata);
        List<AbstractCompactionStrategy> strategies = new ArrayList<>();
        readLock.lock();
        try
        {
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

    public void resume()
    {
        isActive = true;
    }

    /**
     * pause compaction while we cancel all ongoing compactions
     *
     * Separate call from enable/disable to not have to save the enabled-state externally
      */
    public void pause()
    {
        isActive = false;
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
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * return the compaction strategy for the given sstable
     *
     * returns differently based on the repaired status and which vnode the compaction strategy belongs to
     * @param sstable
     * @return
     */
    private AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        int index = getCompactionStrategyIndex(cfs, getDirectories(), sstable);
        readLock.lock();
        try
        {
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
     * @param cfs
     * @param locations
     * @param sstable
     * @return
     */
    public static int getCompactionStrategyIndex(ColumnFamilyStore cfs, Directories locations, SSTableReader sstable)
    {
        if (!cfs.getPartitioner().splitter().isPresent())
            return 0;

        List<PartitionPosition> boundaries = StorageService.getDiskBoundaries(cfs, locations.getWriteableLocations());
        if (boundaries == null)
        {
            Directories.DataDirectory[] directories = locations.getWriteableLocations();

            // try to figure out location based on sstable directory:
            for (int i = 0; i < directories.length; i++)
            {
                Directories.DataDirectory directory = directories[i];
                if (sstable.descriptor.directory.getAbsolutePath().startsWith(directory.location.getAbsolutePath()))
                    return i;
            }
            return 0;
        }

        int pos = Collections.binarySearch(boundaries, sstable.first);
        assert pos < 0; // boundaries are .minkeybound and .maxkeybound so they should never be equal
        return -pos - 1;
    }

    public void shutdown()
    {
        writeLock.lock();
        try
        {
            isActive = false;
            repaired.forEach(AbstractCompactionStrategy::shutdown);
            unrepaired.forEach(AbstractCompactionStrategy::shutdown);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void maybeReload(CFMetaData metadata)
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
    private void reload(CFMetaData metadata)
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
                return count;
            }
        }
        finally
        {
            readLock.unlock();
        }
        return 0;
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
        readLock.lock();
        try
        {
            assert repaired.get(0).getClass().equals(unrepaired.get(0).getClass());
            return repaired.get(0).shouldDefragment();
        }
        finally
        {
            readLock.unlock();
        }
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
            int i = getCompactionStrategyIndex(cfs, getDirectories(), sstable);
            if (sstable.isRepaired())
                repairedRemoved.get(i).add(sstable);
            else
                unrepairedRemoved.get(i).add(sstable);
        }
        for (SSTableReader sstable : added)
        {
            int i = getCompactionStrategyIndex(cfs, getDirectories(), sstable);
            if (sstable.isRepaired())
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
                int index = getCompactionStrategyIndex(cfs, getDirectories(), sstable);
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
            writeLock.unlock();
        }
    }

    private void handleDeletingNotification(SSTableReader deleted)
    {
        readLock.lock();
        try
        {
            getCompactionStrategyFor(deleted).removeSSTable(deleted);
        }
        finally
        {
            readLock.unlock();
        }
    }

    public void handleNotification(INotification notification, Object sender)
    {
        maybeReload(cfs.metadata);
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
                repairedSSTables.get(getCompactionStrategyIndex(cfs, getDirectories(), sstable)).add(sstable);
            else
                unrepairedSSTables.get(getCompactionStrategyIndex(cfs, getDirectories(), sstable)).add(sstable);
        }

        List<ISSTableScanner> scanners = new ArrayList<>(sstables.size());

        readLock.lock();
        try
        {
            for (Range<Token> range : ranges)
            {
                List<ISSTableScanner> repairedScanners = new ArrayList<>();
                List<ISSTableScanner> unrepairedScanners = new ArrayList<>();

                for (int i = 0; i < repairedSSTables.size(); i++)
                {
                    if (!repairedSSTables.get(i).isEmpty())
                        repairedScanners.addAll(repaired.get(i).getScanners(repairedSSTables.get(i), range).scanners);
                }
                for (int i = 0; i < unrepairedSSTables.size(); i++)
                {
                    if (!unrepairedSSTables.get(i).isEmpty())
                        scanners.addAll(unrepaired.get(i).getScanners(unrepairedSSTables.get(i), range).scanners);
                }
                for (ISSTableScanner scanner : Iterables.concat(repairedScanners, unrepairedScanners))
                {
                    if (!scanners.add(scanner))
                        scanner.close();
                }
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
        return getScanners(sstables, Collections.singleton(null));
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        readLock.lock();
        try
        {
            Map<Integer, List<SSTableReader>> groups = sstablesToGroup.stream().collect(Collectors.groupingBy((s) -> getCompactionStrategyIndex(cfs, getDirectories(), s)));
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
        maybeReload(cfs.metadata);
        validateForCompaction(txn.originals(), cfs, getDirectories());
        return getCompactionStrategyFor(txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    private static void validateForCompaction(Iterable<SSTableReader> input, ColumnFamilyStore cfs, Directories directories)
    {
        SSTableReader firstSSTable = Iterables.getFirst(input, null);
        assert firstSSTable != null;
        boolean repaired = firstSSTable.isRepaired();
        int firstIndex = getCompactionStrategyIndex(cfs, directories, firstSSTable);
        for (SSTableReader sstable : input)
        {
            if (sstable.isRepaired() != repaired)
                throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
            if (firstIndex != getCompactionStrategyIndex(cfs, directories, sstable))
                throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
        }
    }

    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput)
    {
        maybeReload(cfs.metadata);
        // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired strategies mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>()
        {
            @Override
            public Collection<AbstractCompactionTask> call() throws Exception
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

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        maybeReload(cfs.metadata);
        validateForCompaction(sstables, cfs, getDirectories());
        readLock.lock();
        try
        {
            return getCompactionStrategyFor(sstables.iterator().next()).getUserDefinedTask(sstables, gcBefore);
        }
        finally
        {
            readLock.unlock();
        }
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

        if (cfs.getPartitioner().splitter().isPresent())
        {
            locations = cfs.getDirectories().getWriteableLocations();
            for (int i = 0; i < locations.length; i++)
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
                                                       LifecycleTransaction txn)
    {
        readLock.lock();
        try
        {
            if (repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
            {
                return unrepaired.get(0).createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, indexes, txn);
            }
            else
            {
                return repaired.get(0).createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, indexes, txn);
            }
        }
        finally
        {
            readLock.unlock();
        }
    }
}
