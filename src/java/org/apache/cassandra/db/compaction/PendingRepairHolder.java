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
import java.util.List;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;

public class PendingRepairHolder extends AbstractStrategyHolder
{
    private final List<PendingRepairManager> managers = new ArrayList<>();
    private final boolean isTransient;

    public PendingRepairHolder(CompactionRealm realm, CompactionStrategyFactory strategyFactory, DestinationRouter router, boolean isTransient)
    {
        super(realm, strategyFactory, router);
        this.isTransient = isTransient;
    }

    @Override
    public void startup()
    {
        managers.forEach(PendingRepairManager::startup);
    }

    @Override
    public void shutdown()
    {
        managers.forEach(PendingRepairManager::shutdown);
    }

    @Override
    public void setStrategyInternal(CompactionParams params, int numTokenPartitions)
    {
        managers.clear();
        for (int i = 0; i < numTokenPartitions; i++)
            managers.add(new PendingRepairManager(realm, strategyFactory, params, isTransient));
    }

    @Override
    public boolean managesRepairedGroup(boolean isRepaired, boolean isPendingRepair, boolean isTransient)
    {
        Preconditions.checkArgument(!isPendingRepair || !isRepaired,
                                    "SSTables cannot be both repaired and pending repair");
        return isPendingRepair && (this.isTransient == isTransient);
    }

    @Override
    public LegacyAbstractCompactionStrategy getStrategyFor(CompactionSSTable sstable)
    {
        Preconditions.checkArgument(managesSSTable(sstable), "Attempting to get compaction strategy from wrong holder");
        return managers.get(router.getIndexForSSTable(sstable)).getOrCreate(sstable);
    }

    @Override
    public Iterable<LegacyAbstractCompactionStrategy> allStrategies()
    {
        return Iterables.concat(Iterables.transform(managers, PendingRepairManager::getStrategies));
    }

    Iterable<LegacyAbstractCompactionStrategy> getStrategiesFor(UUID session)
    {
        List<LegacyAbstractCompactionStrategy> strategies = new ArrayList<>(managers.size());
        for (PendingRepairManager manager : managers)
        {
            LegacyAbstractCompactionStrategy strategy = manager.get(session);
            if (strategy != null)
                strategies.add(strategy);
        }
        return strategies;
    }

    public Iterable<PendingRepairManager> getManagers()
    {
        return managers;
    }

    @Override
    public Collection<TasksSupplier> getBackgroundTaskSuppliers(int gcBefore)
    {
        List<TasksSupplier> suppliers = new ArrayList<>(managers.size());
        for (PendingRepairManager manager : managers)
            suppliers.add(new TasksSupplier(manager.getMaxEstimatedRemainingTasks(), () -> manager.getNextBackgroundTasks(gcBefore)));

        return suppliers;
    }

    @Override
    public Collection<AbstractCompactionTask> getMaximalTasks(int gcBefore, boolean splitOutput)
    {
        List<AbstractCompactionTask> tasks = new ArrayList<>(managers.size());
        for (PendingRepairManager manager : managers)
        {
            tasks.addAll(manager.getMaximalTasks(gcBefore, splitOutput));
        }
        return tasks;
    }

    @Override
    public Collection<AbstractCompactionTask> getUserDefinedTasks(GroupedSSTableContainer sstables, int gcBefore)
    {
        List<AbstractCompactionTask> tasks = new ArrayList<>(managers.size());

        for (int i = 0; i < managers.size(); i++)
        {
            if (sstables.isGroupEmpty(i))
                continue;

            tasks.addAll(managers.get(i).createUserDefinedTasks(sstables.getGroup(i), gcBefore));
        }
        return tasks;
    }

    Collection<AbstractCompactionTask> getNextRepairFinishedTasks()
    {
        List<TasksSupplier> repairFinishedSuppliers = getRepairFinishedTaskSuppliers();
        if (!repairFinishedSuppliers.isEmpty())
        {
            Collections.sort(repairFinishedSuppliers);
            for (TasksSupplier supplier : repairFinishedSuppliers)
            {
                Collection<AbstractCompactionTask> tasks = supplier.getTasks();
                if (!tasks.isEmpty())
                    return tasks;
            }
        }
        return ImmutableList.of();
    }

    private ArrayList<TasksSupplier> getRepairFinishedTaskSuppliers()
    {
        ArrayList<TasksSupplier> suppliers = new ArrayList<>(managers.size());
        for (PendingRepairManager manager : managers)
        {
            int numPending = manager.getNumPendingRepairFinishedTasks();
            if (numPending > 0)
            {
                suppliers.add(new TasksSupplier(numPending, manager::getNextRepairFinishedTasks));
            }
        }

        return suppliers;
    }

    @Override
    public void addSSTables(GroupedSSTableContainer sstables)
    {
        Preconditions.checkArgument(sstables.numGroups() == managers.size());
        for (int i = 0; i < managers.size(); i++)
        {
            if (!sstables.isGroupEmpty(i))
                managers.get(i).addSSTables(sstables.getGroup(i));
        }
    }

    @Override
    public void removeSSTables(GroupedSSTableContainer sstables)
    {
        Preconditions.checkArgument(sstables.numGroups() == managers.size());
        for (int i = 0; i < managers.size(); i++)
        {
            if (!sstables.isGroupEmpty(i))
                managers.get(i).removeSSTables(sstables.getGroup(i));
        }
    }

    @Override
    public void replaceSSTables(GroupedSSTableContainer removed, GroupedSSTableContainer added)
    {
        Preconditions.checkArgument(removed.numGroups() == managers.size());
        Preconditions.checkArgument(added.numGroups() == managers.size());
        for (int i = 0; i < managers.size(); i++)
        {
            if (removed.isGroupEmpty(i) && added.isGroupEmpty(i))
                continue;

            if (removed.isGroupEmpty(i))
                managers.get(i).addSSTables(added.getGroup(i));
            else
                managers.get(i).replaceSSTables(removed.getGroup(i), added.getGroup(i));
        }
    }

    @Override
    public List<ISSTableScanner> getScanners(GroupedSSTableContainer<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        List<ISSTableScanner> scanners = new ArrayList<>(managers.size());
        for (int i = 0; i < managers.size(); i++)
        {
            if (sstables.isGroupEmpty(i))
                continue;

            scanners.addAll(managers.get(i).getScanners(sstables.getGroup(i), ranges));
        }
        return scanners;
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
        Preconditions.checkArgument(repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE,
                                    "PendingRepairHolder can't create sstablewriter with repaired at set");
        Preconditions.checkArgument(pendingRepair != null,
                                    "PendingRepairHolder can't create sstable writer without pendingRepair id");
        // to avoid creating a compaction strategy for the wrong pending repair manager, we get the index based on where the sstable is to be written
        CompactionStrategy strategy = managers.get(router.getIndexForSSTableDirectory(descriptor)).getOrCreate(pendingRepair);
        return strategy.createSSTableMultiWriter(descriptor,
                                                 keyCount,
                                                 repairedAt,
                                                 pendingRepair,
                                                 isTransient,
                                                 collector,
                                                 header,
                                                 indexGroups,
                                                 lifecycleNewTracker);
    }

    public boolean hasDataForSession(UUID sessionID)
    {
        return Iterables.any(managers, prm -> prm.hasDataForSession(sessionID));
    }

    @Override
    public boolean containsSSTable(CompactionSSTable sstable)
    {
        return Iterables.any(managers, prm -> prm.containsSSTable(sstable));
    }

    public int size()
    {
        return managers.size();
    }
}
