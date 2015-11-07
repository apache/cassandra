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

import com.google.common.collect.Iterables;
import org.apache.cassandra.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
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

/**
 * Manages the compaction strategies.
 *
 * Currently has two instances of actual compaction strategies - one for repaired data and one for
 * unrepaired data. This is done to be able to totally separate the different sets of sstables.
 */
public class CompactionStrategyManager implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    private final ColumnFamilyStore cfs;
    private volatile AbstractCompactionStrategy repaired;
    private volatile AbstractCompactionStrategy unrepaired;
    private volatile boolean enabled = true;
    public boolean isActive = true;
    private volatile CompactionParams params;
    /*
        We keep a copy of the schema compaction parameters here to be able to decide if we
        should update the compaction strategy in maybeReloadCompactionStrategy() due to an ALTER.

        If a user changes the local compaction strategy and then later ALTERs a compaction parameter,
        we will use the new compaction parameters.
     */
    private CompactionParams schemaCompactionParams;

    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
        this.cfs = cfs;
        reload(cfs.metadata);
        params = cfs.metadata.params.compaction;
        enabled = params.isEnabled();
    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     *
     */
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

        maybeReload(cfs.metadata);

        if (repaired.getEstimatedRemainingTasks() > unrepaired.getEstimatedRemainingTasks())
        {
            AbstractCompactionTask repairedTask = repaired.getNextBackgroundTask(gcBefore);
            if (repairedTask != null)
                return repairedTask;
            return unrepaired.getNextBackgroundTask(gcBefore);
        }
        else
        {
            AbstractCompactionTask unrepairedTask = unrepaired.getNextBackgroundTask(gcBefore);
            if (unrepairedTask != null)
                return unrepairedTask;
            return repaired.getNextBackgroundTask(gcBefore);
        }
    }

    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    public synchronized void resume()
    {
        isActive = true;
    }

    /**
     * pause compaction while we cancel all ongoing compactions
     *
     * Separate call from enable/disable to not have to save the enabled-state externally
      */
    public synchronized void pause()
    {
        isActive = false;
    }


    private void startup()
    {
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
        {
            if (sstable.openReason != SSTableReader.OpenReason.EARLY)
                getCompactionStrategyFor(sstable).addSSTable(sstable);
        }
        repaired.startup();
        unrepaired.startup();
    }

    /**
     * return the compaction strategy for the given sstable
     *
     * returns differently based on the repaired status
     * @param sstable
     * @return
     */
    private AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        if (sstable.isRepaired())
            return repaired;
        else
            return unrepaired;
    }

    public void shutdown()
    {
        isActive = false;
        repaired.shutdown();
        unrepaired.shutdown();
    }

    public synchronized void maybeReload(CFMetaData metadata)
    {
        // compare the old schema configuration to the new one, ignore any locally set changes.
        if (metadata.params.compaction.equals(schemaCompactionParams))
            return;
        reload(metadata);
    }

    /**
     * Reload the compaction strategies
     *
     * Called after changing configuration and at startup.
     * @param metadata
     */
    public synchronized void reload(CFMetaData metadata)
    {
        boolean disabledWithJMX = !enabled && shouldBeEnabled();
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
        if (repaired instanceof LeveledCompactionStrategy && unrepaired instanceof LeveledCompactionStrategy)
        {
            int count = 0;
            count += ((LeveledCompactionStrategy)repaired).getLevelSize(0);
            count += ((LeveledCompactionStrategy)unrepaired).getLevelSize(0);
            return count;
        }
        return 0;
    }

    public synchronized int[] getSSTableCountPerLevel()
    {
        if (repaired instanceof LeveledCompactionStrategy && unrepaired instanceof LeveledCompactionStrategy)
        {
            int [] res = new int[LeveledManifest.MAX_LEVEL_COUNT];
            int[] repairedCountPerLevel = ((LeveledCompactionStrategy) repaired).getAllLevelSize();
            res = sumArrays(res, repairedCountPerLevel);
            int[] unrepairedCountPerLevel = ((LeveledCompactionStrategy) unrepaired).getAllLevelSize();
            res = sumArrays(res, unrepairedCountPerLevel);
            return res;
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
        assert repaired.getClass().equals(unrepaired.getClass());
        return repaired.shouldDefragment();
    }

    public Directories getDirectories()
    {
        assert repaired.getClass().equals(unrepaired.getClass());
        return repaired.getDirectories();
    }

    public synchronized void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
            for (SSTableReader sstable : flushedNotification.added)
            {
                if (sstable.isRepaired())
                    repaired.addSSTable(sstable);
                else
                    unrepaired.addSSTable(sstable);
            }
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
            Set<SSTableReader> repairedRemoved = new HashSet<>();
            Set<SSTableReader> repairedAdded = new HashSet<>();
            Set<SSTableReader> unrepairedRemoved = new HashSet<>();
            Set<SSTableReader> unrepairedAdded = new HashSet<>();

            for (SSTableReader sstable : listChangedNotification.removed)
            {
                if (sstable.isRepaired())
                    repairedRemoved.add(sstable);
                else
                    unrepairedRemoved.add(sstable);
            }
            for (SSTableReader sstable : listChangedNotification.added)
            {
                if (sstable.isRepaired())
                    repairedAdded.add(sstable);
                else
                    unrepairedAdded.add(sstable);
            }
            if (!repairedRemoved.isEmpty())
            {
                repaired.replaceSSTables(repairedRemoved, repairedAdded);
            }
            else
            {
                for (SSTableReader sstable : repairedAdded)
                    repaired.addSSTable(sstable);
            }

            if (!unrepairedRemoved.isEmpty())
            {
                unrepaired.replaceSSTables(unrepairedRemoved, unrepairedAdded);
            }
            else
            {
                for (SSTableReader sstable : unrepairedAdded)
                    unrepaired.addSSTable(sstable);
            }
        }
        else if (notification instanceof SSTableRepairStatusChanged)
        {
            for (SSTableReader sstable : ((SSTableRepairStatusChanged) notification).sstable)
            {
                if (sstable.isRepaired())
                {
                    unrepaired.removeSSTable(sstable);
                    repaired.addSSTable(sstable);
                }
                else
                {
                    repaired.removeSSTable(sstable);
                    unrepaired.addSSTable(sstable);
                }
            }
        }
        else if (notification instanceof SSTableDeletingNotification)
        {
            SSTableReader sstable = ((SSTableDeletingNotification)notification).deleting;
            if (sstable.isRepaired())
                repaired.removeSSTable(sstable);
            else
                unrepaired.removeSSTable(sstable);
        }
    }

    public void enable()
    {
        if (repaired != null)
            repaired.enable();
        if (unrepaired != null)
            unrepaired.enable();
        // enable this last to make sure the strategies are ready to get calls.
        enabled = true;
    }

    public void disable()
    {
        // disable this first avoid asking disabled strategies for compaction tasks
        enabled = false;
        if (repaired != null)
            repaired.disable();
        if (unrepaired != null)
            unrepaired.disable();
    }

    /**
     * Create ISSTableScanner from the given sstables
     *
     * Delegates the call to the compaction strategies to allow LCS to create a scanner
     * @param sstables
     * @param range
     * @return
     */
    @SuppressWarnings("resource")
    public synchronized AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables,  Collection<Range<Token>> ranges)
    {
        List<SSTableReader> repairedSSTables = new ArrayList<>();
        List<SSTableReader> unrepairedSSTables = new ArrayList<>();
        for (SSTableReader sstable : sstables)
        {
            if (sstable.isRepaired())
                repairedSSTables.add(sstable);
            else
                unrepairedSSTables.add(sstable);
        }

        Set<ISSTableScanner> scanners = new HashSet<>(sstables.size());

        for (Range<Token> range : ranges)
        {
            AbstractCompactionStrategy.ScannerList repairedScanners = repaired.getScanners(repairedSSTables, range);
            AbstractCompactionStrategy.ScannerList unrepairedScanners = unrepaired.getScanners(unrepairedSSTables, range);

            for (ISSTableScanner scanner : Iterables.concat(repairedScanners.scanners, unrepairedScanners.scanners))
            {
                if (!scanners.add(scanner))
                    scanner.close();
            }
        }

        return new AbstractCompactionStrategy.ScannerList(new ArrayList<>(scanners));
    }

    public synchronized AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, Collections.singleton(null));
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        return unrepaired.groupSSTablesForAntiCompaction(sstablesToGroup);
    }

    public long getMaxSSTableBytes()
    {
        return unrepaired.getMaxSSTableBytes();
    }

    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        return getCompactionStrategyFor(txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput)
    {
        // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired strategies mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>()
        {
            @Override
            public Collection<AbstractCompactionTask> call() throws Exception
            {
                synchronized (CompactionStrategyManager.this)
                {
                    Collection<AbstractCompactionTask> repairedTasks = repaired.getMaximalTask(gcBefore, splitOutput);
                    Collection<AbstractCompactionTask> unrepairedTasks = unrepaired.getMaximalTask(gcBefore, splitOutput);

                    if (repairedTasks == null && unrepairedTasks == null)
                        return null;

                    if (repairedTasks == null)
                        return unrepairedTasks;
                    if (unrepairedTasks == null)
                        return repairedTasks;

                    List<AbstractCompactionTask> tasks = new ArrayList<>();
                    tasks.addAll(repairedTasks);
                    tasks.addAll(unrepairedTasks);
                    return tasks;
                }
            }
        }, false, false);
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        return getCompactionStrategyFor(sstables.iterator().next()).getUserDefinedTask(sstables, gcBefore);
    }

    public int getEstimatedRemainingTasks()
    {
        int tasks = 0;
        tasks += repaired.getEstimatedRemainingTasks();
        tasks += unrepaired.getEstimatedRemainingTasks();

        return tasks;
    }

    public boolean shouldBeEnabled()
    {
        return params.isEnabled();
    }

    public String getName()
    {
        return unrepaired.getName();
    }

    public List<AbstractCompactionStrategy> getStrategies()
    {
        return Arrays.asList(repaired, unrepaired);
    }

    public synchronized void setNewLocalCompactionStrategy(CompactionParams params)
    {
        logger.info("Switching local compaction strategy from {} to {}}", this.params, params);
        setStrategy(params);
        if (shouldBeEnabled())
            enable();
        else
            disable();
        startup();
    }

    private void setStrategy(CompactionParams params)
    {
        if (repaired != null)
            repaired.shutdown();
        if (unrepaired != null)
            unrepaired.shutdown();
        repaired = CFMetaData.createCompactionStrategyInstance(cfs, params);
        unrepaired = CFMetaData.createCompactionStrategyInstance(cfs, params);
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
        if (repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
        {
            return unrepaired.createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, indexes, txn);
        }
        else
        {
            return repaired.createSSTableMultiWriter(descriptor, keyCount, repairedAt, collector, header, indexes, txn);
        }
    }
}
