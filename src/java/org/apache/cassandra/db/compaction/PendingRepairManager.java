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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;

/**
 * Companion to CompactionStrategyManager which manages the sstables marked pending repair.
 *
 * SSTables are classified as pending repair by the anti-compaction performed at the beginning
 * of an incremental repair, or when they're streamed in with a pending repair id. This prevents
 * unrepaired / pending repaired sstables from being compacted together. Once the repair session
 * has completed, or failed, sstables will be re-classified as part of the compaction process.
 */
class PendingRepairManager
{
    private static final Logger logger = LoggerFactory.getLogger(PendingRepairManager.class);

    private final ColumnFamilyStore cfs;
    private final CompactionParams params;
    private volatile ImmutableMap<UUID, AbstractCompactionStrategy> strategies = ImmutableMap.of();

    PendingRepairManager(ColumnFamilyStore cfs, CompactionParams params)
    {
        this.cfs = cfs;
        this.params = params;
    }

    private ImmutableMap.Builder<UUID, AbstractCompactionStrategy> mapBuilder()
    {
        return ImmutableMap.builder();
    }

    AbstractCompactionStrategy get(UUID id)
    {
        return strategies.get(id);
    }

    AbstractCompactionStrategy get(SSTableReader sstable)
    {
        assert sstable.isPendingRepair();
        return get(sstable.getSSTableMetadata().pendingRepair);
    }

    AbstractCompactionStrategy getOrCreate(UUID id)
    {
        assert id != null;
        AbstractCompactionStrategy strategy = get(id);
        if (strategy == null)
        {
            synchronized (this)
            {
                strategy = get(id);

                if (strategy == null)
                {
                    logger.debug("Creating {}.{} compaction strategy for pending repair: {}", cfs.metadata.keyspace, cfs.metadata.name, id);
                    strategy = cfs.createCompactionStrategyInstance(params);
                    strategies = mapBuilder().putAll(strategies).put(id, strategy).build();
                }
            }
        }
        return strategy;
    }

    AbstractCompactionStrategy getOrCreate(SSTableReader sstable)
    {
        assert sstable.isPendingRepair();
        return getOrCreate(sstable.getSSTableMetadata().pendingRepair);
    }

    private synchronized void removeSession(UUID sessionID)
    {
        if (!strategies.containsKey(sessionID))
            return;

        logger.debug("Removing compaction strategy for pending repair {} on  {}.{}", sessionID, cfs.metadata.keyspace, cfs.metadata.name);
        strategies = ImmutableMap.copyOf(Maps.filterKeys(strategies, k -> !k.equals(sessionID)));
    }

    synchronized void removeSSTable(SSTableReader sstable)
    {
        for (AbstractCompactionStrategy strategy : strategies.values())
            strategy.removeSSTable(sstable);
    }

    synchronized void addSSTable(SSTableReader sstable)
    {
        getOrCreate(sstable).addSSTable(sstable);
    }

    synchronized void replaceSSTables(Set<SSTableReader> removed, Set<SSTableReader> added)
    {
        if (removed.isEmpty() && added.isEmpty())
            return;

        // left=removed, right=added
        Map<UUID, Pair<Set<SSTableReader>, Set<SSTableReader>>> groups = new HashMap<>();
        for (SSTableReader sstable : removed)
        {
            UUID sessionID = sstable.getSSTableMetadata().pendingRepair;
            if (!groups.containsKey(sessionID))
            {
                groups.put(sessionID, Pair.create(new HashSet<>(), new HashSet<>()));
            }
            groups.get(sessionID).left.add(sstable);
        }

        for (SSTableReader sstable : added)
        {
            UUID sessionID = sstable.getSSTableMetadata().pendingRepair;
            if (!groups.containsKey(sessionID))
            {
                groups.put(sessionID, Pair.create(new HashSet<>(), new HashSet<>()));
            }
            groups.get(sessionID).right.add(sstable);
        }

        for (Map.Entry<UUID, Pair<Set<SSTableReader>, Set<SSTableReader>>> entry : groups.entrySet())
        {
            AbstractCompactionStrategy strategy = getOrCreate(entry.getKey());
            Set<SSTableReader> groupRemoved = entry.getValue().left;
            Set<SSTableReader> groupAdded = entry.getValue().right;

            if (!groupRemoved.isEmpty())
                strategy.replaceSSTables(groupRemoved, groupAdded);
            else
                strategy.addSSTables(groupAdded);
        }
    }

    synchronized void startup()
    {
        strategies.values().forEach(AbstractCompactionStrategy::startup);
    }

    synchronized void shutdown()
    {
        strategies.values().forEach(AbstractCompactionStrategy::shutdown);
    }

    synchronized void enable()
    {
        strategies.values().forEach(AbstractCompactionStrategy::enable);
    }

    synchronized void disable()
    {
        strategies.values().forEach(AbstractCompactionStrategy::disable);
    }

    private int getEstimatedRemainingTasks(UUID sessionID, AbstractCompactionStrategy strategy)
    {
        if (canCleanup(sessionID))
        {
            return 0;
        }
        else
        {
            return strategy.getEstimatedRemainingTasks();
        }
    }

    int getEstimatedRemainingTasks()
    {
        int tasks = 0;
        for (Map.Entry<UUID, AbstractCompactionStrategy> entry : strategies.entrySet())
        {
            tasks += getEstimatedRemainingTasks(entry.getKey(), entry.getValue());
        }
        return tasks;
    }

    /**
     * @return the highest max remaining tasks of all contained compaction strategies
     */
    int getMaxEstimatedRemainingTasks()
    {
        int tasks = 0;
        for (Map.Entry<UUID, AbstractCompactionStrategy> entry : strategies.entrySet())
        {
            tasks = Math.max(tasks, getEstimatedRemainingTasks(entry.getKey(), entry.getValue()));
        }
        return tasks;
    }

    @SuppressWarnings("resource")
    private RepairFinishedCompactionTask getRepairFinishedCompactionTask(UUID sessionID)
    {
        Set<SSTableReader> sstables = get(sessionID).getSSTables();
        long repairedAt = ActiveRepairService.instance.consistent.local.getFinalSessionRepairedAt(sessionID);
        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        return txn == null ? null : new RepairFinishedCompactionTask(cfs, txn, sessionID, repairedAt);
    }

    synchronized int getNumPendingRepairFinishedTasks()
    {
        int count = 0;
        for (UUID sessionID : strategies.keySet())
        {
            if (canCleanup(sessionID))
            {
                count++;
            }
        }
        return count;
    }

    synchronized AbstractCompactionTask getNextRepairFinishedTask()
    {
        for (UUID sessionID : strategies.keySet())
        {
            if (canCleanup(sessionID))
            {
                return getRepairFinishedCompactionTask(sessionID);
            }
        }
        return null;
    }

    synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (strategies.isEmpty())
            return null;

        Map<UUID, Integer> numTasks = new HashMap<>(strategies.size());
        ArrayList<UUID> sessions = new ArrayList<>(strategies.size());
        for (Map.Entry<UUID, AbstractCompactionStrategy> entry : strategies.entrySet())
        {
            if (canCleanup(entry.getKey()))
            {
                continue;
            }
            numTasks.put(entry.getKey(), getEstimatedRemainingTasks(entry.getKey(), entry.getValue()));
            sessions.add(entry.getKey());
        }

        // we want the session with the most compactions at the head of the list
        sessions.sort((o1, o2) -> numTasks.get(o2) - numTasks.get(o1));

        UUID sessionID = sessions.get(0);
        return get(sessionID).getNextBackgroundTask(gcBefore);
    }

    synchronized Collection<AbstractCompactionTask> getMaximalTasks(int gcBefore, boolean splitOutput)
    {
        if (strategies.isEmpty())
            return null;

        List<AbstractCompactionTask> maximalTasks = new ArrayList<>(strategies.size());
        for (Map.Entry<UUID, AbstractCompactionStrategy> entry : strategies.entrySet())
        {
            if (canCleanup(entry.getKey()))
            {
                maximalTasks.add(getRepairFinishedCompactionTask(entry.getKey()));
            }
            else
            {
                Collection<AbstractCompactionTask> tasks = entry.getValue().getMaximalTask(gcBefore, splitOutput);
                if (tasks != null)
                    maximalTasks.addAll(tasks);
            }
        }
        return !maximalTasks.isEmpty() ? maximalTasks : null;
    }

    Collection<AbstractCompactionStrategy> getStrategies()
    {
        return strategies.values();
    }

    Set<UUID> getSessions()
    {
        return strategies.keySet();
    }

    boolean canCleanup(UUID sessionID)
    {
        return !ActiveRepairService.instance.consistent.local.isSessionInProgress(sessionID);
    }

    /**
     * calling this when underlying strategy is not LeveledCompactionStrategy is an error
     */
    synchronized int[] getSSTableCountPerLevel()
    {
        int [] res = new int[LeveledManifest.MAX_LEVEL_COUNT];
        for (AbstractCompactionStrategy strategy : strategies.values())
        {
            assert strategy instanceof LeveledCompactionStrategy;
            int[] counts = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
            res = CompactionStrategyManager.sumArrays(res, counts);
        }
        return res;
    }

    @SuppressWarnings("resource")
    synchronized Set<ISSTableScanner> getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        if (sstables.isEmpty())
        {
            return Collections.emptySet();
        }

        Map<UUID, Set<SSTableReader>> sessionSSTables = new HashMap<>();
        for (SSTableReader sstable : sstables)
        {
            UUID sessionID = sstable.getSSTableMetadata().pendingRepair;
            assert sessionID != null;
            sessionSSTables.computeIfAbsent(sessionID, k -> new HashSet<>()).add(sstable);
        }

        Set<ISSTableScanner> scanners = new HashSet<>(sessionSSTables.size());
        for (Map.Entry<UUID, Set<SSTableReader>> entry : sessionSSTables.entrySet())
        {
            scanners.addAll(get(entry.getKey()).getScanners(entry.getValue(), ranges).scanners);
        }
        return scanners;
    }

    public boolean hasStrategy(AbstractCompactionStrategy strategy)
    {
        return strategies.values().contains(strategy);
    }

    public Collection<AbstractCompactionTask> createUserDefinedTasks(List<SSTableReader> sstables, int gcBefore)
    {
        Map<UUID, List<SSTableReader>> group = sstables.stream().collect(Collectors.groupingBy(s -> s.getSSTableMetadata().pendingRepair));
        return group.entrySet().stream().map(g -> strategies.get(g.getKey()).getUserDefinedTask(g.getValue(), gcBefore)).collect(Collectors.toList());
    }

    /**
     * promotes/demotes sstables involved in a consistent repair that has been finalized, or failed
     */
    class RepairFinishedCompactionTask extends AbstractCompactionTask
    {
        private final UUID sessionID;
        private final long repairedAt;

        RepairFinishedCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction transaction, UUID sessionID, long repairedAt)
        {
            super(cfs, transaction);
            this.sessionID = sessionID;
            this.repairedAt = repairedAt;
        }

        @VisibleForTesting
        UUID getSessionID()
        {
            return sessionID;
        }

        protected void runMayThrow() throws Exception
        {
            for (SSTableReader sstable : transaction.originals())
            {
                logger.debug("Setting repairedAt to {} on {} for {}", repairedAt, sstable, sessionID);
                sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, repairedAt, ActiveRepairService.NO_PENDING_REPAIR);
                sstable.reloadSSTableMetadata();
            }
            cfs.getTracker().notifySSTableRepairedStatusChanged(transaction.originals());
            transaction.abort();
        }

        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables)
        {
            throw new UnsupportedOperationException();
        }

        protected int executeInternal(CompactionManager.CompactionExecutorStatsCollector collector)
        {
            run();
            return transaction.originals().size();
        }

        public int execute(CompactionManager.CompactionExecutorStatsCollector collector)
        {
            try
            {
                return super.execute(collector);
            }
            finally
            {
                removeSession(sessionID);
            }
        }
    }

}
