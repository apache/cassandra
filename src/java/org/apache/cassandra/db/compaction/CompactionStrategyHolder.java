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
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

public class CompactionStrategyHolder extends AbstractStrategyHolder
{
    private final List<AbstractCompactionStrategy> strategies = new ArrayList<>();
    private final boolean isRepaired;

    public CompactionStrategyHolder(ColumnFamilyStore cfs, DestinationRouter router, boolean isRepaired)
    {
        super(cfs, router);
        this.isRepaired = isRepaired;
    }

    @Override
    public void startup()
    {
        strategies.forEach(AbstractCompactionStrategy::startup);
    }

    @Override
    public void shutdown()
    {
        strategies.forEach(AbstractCompactionStrategy::shutdown);
    }

    @Override
    public void setStrategyInternal(CompactionParams params, int numTokenPartitions)
    {
        strategies.clear();
        for (int i = 0; i < numTokenPartitions; i++)
            strategies.add(cfs.createCompactionStrategyInstance(params));
    }

    @Override
    public boolean managesRepairedGroup(boolean isRepaired, boolean isPendingRepair, boolean isTransient)
    {
        if (!isPendingRepair)
        {
            Preconditions.checkArgument(!isTransient, "isTransient can only be true for sstables pending repairs");
            return this.isRepaired == isRepaired;
        }
        else
        {
            Preconditions.checkArgument(!isRepaired, "SSTables cannot be both repaired and pending repair");
            return false;

        }
    }

    @Override
    public AbstractCompactionStrategy getStrategyFor(SSTableReader sstable)
    {
        Preconditions.checkArgument(managesSSTable(sstable), "Attempting to get compaction strategy from wrong holder");
        return strategies.get(router.getIndexForSSTable(sstable));
    }

    @Override
    public Iterable<AbstractCompactionStrategy> allStrategies()
    {
        return strategies;
    }

    @Override
    public Collection<TaskSupplier> getBackgroundTaskSuppliers(long gcBefore)
    {
        List<TaskSupplier> suppliers = new ArrayList<>(strategies.size());
        for (AbstractCompactionStrategy strategy : strategies)
            suppliers.add(new TaskSupplier(strategy.getEstimatedRemainingTasks(), () -> strategy.getNextBackgroundTask(gcBefore)));

        return suppliers;
    }

    @Override
    public Collection<AbstractCompactionTask> getMaximalTasks(long gcBefore, boolean splitOutput)
    {
        List<AbstractCompactionTask> tasks = new ArrayList<>(strategies.size());
        for (AbstractCompactionStrategy strategy : strategies)
        {
            Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
            if (task != null)
                tasks.addAll(task);
        }
        return tasks;
    }

    @Override
    public Collection<AbstractCompactionTask> getUserDefinedTasks(GroupedSSTableContainer sstables, long gcBefore)
    {
        List<AbstractCompactionTask> tasks = new ArrayList<>(strategies.size());
        for (int i = 0; i < strategies.size(); i++)
        {
            if (sstables.isGroupEmpty(i))
                continue;

            tasks.add(strategies.get(i).getUserDefinedTask(sstables.getGroup(i), gcBefore));
        }
        return tasks;
    }

    @Override
    public void addSSTables(GroupedSSTableContainer sstables)
    {
        Preconditions.checkArgument(sstables.numGroups() == strategies.size());
        for (int i = 0; i < strategies.size(); i++)
        {
            if (!sstables.isGroupEmpty(i))
                strategies.get(i).addSSTables(sstables.getGroup(i));
        }
    }

    @Override
    public void removeSSTables(GroupedSSTableContainer sstables)
    {
        Preconditions.checkArgument(sstables.numGroups() == strategies.size());
        for (int i = 0; i < strategies.size(); i++)
        {
            if (!sstables.isGroupEmpty(i))
                strategies.get(i).removeSSTables(sstables.getGroup(i));
        }
    }

    @Override
    public void replaceSSTables(GroupedSSTableContainer removed, GroupedSSTableContainer added)
    {
        Preconditions.checkArgument(removed.numGroups() == strategies.size());
        Preconditions.checkArgument(added.numGroups() == strategies.size());
        for (int i = 0; i < strategies.size(); i++)
        {
            if (removed.isGroupEmpty(i) && added.isGroupEmpty(i))
                continue;

            if (removed.isGroupEmpty(i))
                strategies.get(i).addSSTables(added.getGroup(i));
            else
                strategies.get(i).replaceSSTables(removed.getGroup(i), added.getGroup(i));
        }
    }

    public AbstractCompactionStrategy first()
    {
        return strategies.get(0);
    }

    @Override
    public List<ISSTableScanner> getScanners(GroupedSSTableContainer sstables, Collection<Range<Token>> ranges)
    {
        List<ISSTableScanner> scanners = new ArrayList<>(strategies.size());
        for (int i = 0; i < strategies.size(); i++)
        {
            if (sstables.isGroupEmpty(i))
                continue;

            scanners.addAll(strategies.get(i).getScanners(sstables.getGroup(i), ranges).scanners);
        }
        return scanners;
    }

    Collection<Collection<SSTableReader>> groupForAnticompaction(Iterable<SSTableReader> sstables)
    {
        Preconditions.checkState(!isRepaired);
        GroupedSSTableContainer group = createGroupedSSTableContainer();
        sstables.forEach(group::add);

        Collection<Collection<SSTableReader>> anticompactionGroups = new ArrayList<>();
        for (int i = 0; i < strategies.size(); i++)
        {
            if (group.isGroupEmpty(i))
                continue;

            anticompactionGroups.addAll(strategies.get(i).groupSSTablesForAntiCompaction(group.getGroup(i)));
        }

        return anticompactionGroups;
    }

    @Override
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
        if (isRepaired)
        {
            Preconditions.checkArgument(repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE,
                                        "Repaired CompactionStrategyHolder can't create unrepaired sstable writers");
        }
        else
        {
            Preconditions.checkArgument(repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE,
                                        "Unrepaired CompactionStrategyHolder can't create repaired sstable writers");
        }
        Preconditions.checkArgument(pendingRepair == null,
                                    "CompactionStrategyHolder can't create sstable writer with pendingRepair id");
        // to avoid creating a compaction strategy for the wrong pending repair manager, we get the index based on where the sstable is to be written
        AbstractCompactionStrategy strategy = strategies.get(router.getIndexForSSTableDirectory(descriptor));
        return strategy.createSSTableMultiWriter(descriptor,
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

    @Override
    public int getStrategyIndex(AbstractCompactionStrategy strategy)
    {
        return strategies.indexOf(strategy);
    }

    @Override
    public boolean containsSSTable(SSTableReader sstable)
    {
        return Iterables.any(strategies, acs -> acs.getSSTables().contains(sstable));
    }

    public int getEstimatedRemainingTasks()
    {
        int tasks = 0;
        for (AbstractCompactionStrategy strategy : strategies)
        {
            tasks += strategy.getEstimatedRemainingTasks();
        }
        return tasks;
    }
}
