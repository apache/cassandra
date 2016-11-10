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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;

public final class WrappingCompactionStrategy extends AbstractCompactionStrategy implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(WrappingCompactionStrategy.class);
    private volatile AbstractCompactionStrategy repaired;
    private volatile AbstractCompactionStrategy unrepaired;
    /*
        We keep a copy of the schema compaction options and class here to be able to decide if we
        should update the compaction strategy in maybeReloadCompactionStrategy() due to an ALTER.

        If a user changes the local compaction strategy and then later ALTERs a compaction option,
        we will use the new compaction options.
     */
    private Map<String, String> schemaCompactionOptions;
    private Class<?> schemaCompactionStrategyClass;

    public WrappingCompactionStrategy(ColumnFamilyStore cfs)
    {
        super(cfs, cfs.metadata.compactionStrategyOptions);
        reloadCompactionStrategy(cfs.metadata);
        cfs.getTracker().subscribe(this);
        logger.trace("{} subscribed to the data tracker.", this);
    }

    @Override
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

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

    @Override
    public Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore, final boolean splitOutput)
    {
        // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired strategies mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>()
        {
            @Override
            public Collection<AbstractCompactionTask> call() throws Exception
            {
                synchronized (WrappingCompactionStrategy.this)
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
        }, false);
    }

    @Override
    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, final int gcBefore, long maxSSTableBytes)
    {
        assert txn.originals().size() > 0;
        boolean repairedSSTables = txn.originals().iterator().next().isRepaired();
        for (SSTableReader sstable : txn.originals())
            if (repairedSSTables != sstable.isRepaired())
                throw new RuntimeException("Can't mix repaired and unrepaired sstables in a compaction");

        if (repairedSSTables)
            return repaired.getCompactionTask(txn, gcBefore, maxSSTableBytes);
        else
            return unrepaired.getCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    @Override
    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        assert !sstables.isEmpty();
        boolean userDefinedInRepaired = sstables.iterator().next().isRepaired();
        for (SSTableReader sstable : sstables)
        {
            if (userDefinedInRepaired != sstable.isRepaired())
            {
                logger.error("You can't mix repaired and unrepaired sstables in a user defined compaction");
                return null;
            }
        }
        if (userDefinedInRepaired)
            return repaired.getUserDefinedTask(sstables, gcBefore);
        else
            return unrepaired.getUserDefinedTask(sstables, gcBefore);
    }

    @Override
    public synchronized int getEstimatedRemainingTasks()
    {
        assert repaired.getClass().equals(unrepaired.getClass());
        return repaired.getEstimatedRemainingTasks() + unrepaired.getEstimatedRemainingTasks();
    }

    @Override
    public synchronized long getMaxSSTableBytes()
    {
        assert repaired.getClass().equals(unrepaired.getClass());
        return unrepaired.getMaxSSTableBytes();
    }

    public synchronized void maybeReloadCompactionStrategy(CFMetaData metadata)
    {
        // compare the old schema configuration to the new one, ignore any locally set changes.
        if (metadata.compactionStrategyClass.equals(schemaCompactionStrategyClass) &&
            metadata.compactionStrategyOptions.equals(schemaCompactionOptions))
            return;
        reloadCompactionStrategy(metadata);
    }

    public synchronized void reloadCompactionStrategy(CFMetaData metadata)
    {
        boolean disabledWithJMX = !enabled && shouldBeEnabled();
        setStrategy(metadata.compactionStrategyClass, metadata.compactionStrategyOptions);
        schemaCompactionOptions = ImmutableMap.copyOf(metadata.compactionStrategyOptions);
        schemaCompactionStrategyClass = repaired.getClass();

        if (disabledWithJMX || !shouldBeEnabled())
            disable();
        else
            enable();
        startup();
    }

    public synchronized int getUnleveledSSTables()
    {
        if (this.repaired instanceof LeveledCompactionStrategy && this.unrepaired instanceof LeveledCompactionStrategy)
        {
            return ((LeveledCompactionStrategy)repaired).getLevelSize(0) + ((LeveledCompactionStrategy)unrepaired).getLevelSize(0);
        }
        return 0;
    }

    public synchronized int[] getSSTableCountPerLevel()
    {
        if (this.repaired instanceof LeveledCompactionStrategy && this.unrepaired instanceof LeveledCompactionStrategy)
        {
            int [] repairedCountPerLevel = ((LeveledCompactionStrategy) repaired).getAllLevelSize();
            int [] unrepairedCountPerLevel = ((LeveledCompactionStrategy) unrepaired).getAllLevelSize();
            return sumArrays(repairedCountPerLevel, unrepairedCountPerLevel);
        }
        return null;
    }

    public static int [] sumArrays(int[] a, int [] b)
    {
        int [] res = new int[Math.max(a.length, b.length)];
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

    @Override
    public boolean shouldDefragment()
    {
        assert repaired.getClass().equals(unrepaired.getClass());
        return repaired.shouldDefragment();
    }

    @Override
    public String getName()
    {
        assert repaired.getClass().equals(unrepaired.getClass());
        return repaired.getName();
    }

    @Override
    public void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        throw new UnsupportedOperationException("Can't replace sstables in the wrapping compaction strategy");
    }

    @Override
    public void addSSTable(SSTableReader added)
    {
        throw new UnsupportedOperationException("Can't add sstables to the wrapping compaction strategy");
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        throw new UnsupportedOperationException("Can't remove sstables from the wrapping compaction strategy");
    }

    public synchronized void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
            if (flushedNotification.added.isRepaired())
                repaired.addSSTable(flushedNotification.added);
            else
                unrepaired.addSSTable(flushedNotification.added);
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

    @Override
    public List<SSTableReader> filterSSTablesForReads(List<SSTableReader> sstables)
    {
        // todo: union of filtered sstables or intersection?
        return unrepaired.filterSSTablesForReads(repaired.filterSSTablesForReads(sstables));
    }

    @Override
    public synchronized void startup()
    {
        super.startup();
        for (SSTableReader sstable : cfs.getSSTables())
        {
            if (sstable.openReason != SSTableReader.OpenReason.EARLY)
            {
                if (sstable.isRepaired())
                    repaired.addSSTable(sstable);
                else
                    unrepaired.addSSTable(sstable);
            }
        }
        repaired.startup();
        unrepaired.startup();
    }

    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
        repaired.shutdown();
        unrepaired.shutdown();
    }

    @Override
    public void enable()
    {
        if (repaired != null)
            repaired.enable();
        if (unrepaired != null)
            unrepaired.enable();
        // enable this last to make sure the strategies are ready to get calls.
        super.enable();
    }

    @Override
    public void disable()
    {
        // disable this first avoid asking disabled strategies for compaction tasks
        super.disable();
        if (repaired != null)
            repaired.disable();
        if (unrepaired != null)
            unrepaired.disable();
    }

    @Override
    @SuppressWarnings("resource")
    public synchronized ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        List<SSTableReader> repairedSSTables = new ArrayList<>();
        List<SSTableReader> unrepairedSSTables = new ArrayList<>();
        for (SSTableReader sstable : sstables)
            if (sstable.isRepaired())
                repairedSSTables.add(sstable);
            else
                unrepairedSSTables.add(sstable);
        ScannerList repairedScanners = repaired.getScanners(repairedSSTables, range);
        ScannerList unrepairedScanners = unrepaired.getScanners(unrepairedSSTables, range);
        List<ISSTableScanner> scanners = new ArrayList<>(repairedScanners.scanners.size() + unrepairedScanners.scanners.size());
        scanners.addAll(repairedScanners.scanners);
        scanners.addAll(unrepairedScanners.scanners);
        return new ScannerList(scanners);
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        return unrepaired.groupSSTablesForAntiCompaction(sstablesToGroup);
    }

    public List<AbstractCompactionStrategy> getWrappedStrategies()
    {
        return Arrays.asList(repaired, unrepaired);
    }

    public synchronized void setNewLocalCompactionStrategy(Class<? extends AbstractCompactionStrategy> compactionStrategyClass, Map<String, String> options)
    {
        logger.info("Switching local compaction strategy from {} to {} with options={}", repaired == null ? "null" : repaired.getClass(), compactionStrategyClass, options);
        setStrategy(compactionStrategyClass, options);
        if (shouldBeEnabled())
            enable();
        else
            disable();
        startup();
    }

    private void setStrategy(Class<? extends AbstractCompactionStrategy> compactionStrategyClass, Map<String, String> options)
    {
        if (repaired != null)
            repaired.shutdown();
        if (unrepaired != null)
            unrepaired.shutdown();
        repaired = CFMetaData.createCompactionStrategyInstance(compactionStrategyClass, cfs, options);
        unrepaired = CFMetaData.createCompactionStrategyInstance(compactionStrategyClass, cfs, options);
        this.options = ImmutableMap.copyOf(options);
    }
}
