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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;

/**
 * This class manages the sstables marked pending repair so that they can be assigned to legacy compaction
 * strategies via the legacy strategy container or manager.
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
    private final CompactionStrategyFactory strategyFactory;
    private final CompactionParams params;
    private final boolean isTransient;
    private volatile ImmutableMap<UUID, LegacyAbstractCompactionStrategy> strategies = ImmutableMap.of();

    /**
     * Indicates we're being asked to do something with an sstable that isn't marked pending repair
     */
    public static class IllegalSSTableArgumentException extends IllegalArgumentException
    {
        public IllegalSSTableArgumentException(String s)
        {
            super(s);
        }
    }

    PendingRepairManager(ColumnFamilyStore cfs, CompactionStrategyFactory strategyFactory, CompactionParams params, boolean isTransient)
    {
        this.cfs = cfs;
        this.strategyFactory = strategyFactory;
        this.params = params;
        this.isTransient = isTransient;
    }

    private ImmutableMap.Builder<UUID, LegacyAbstractCompactionStrategy> mapBuilder()
    {
        return ImmutableMap.builder();
    }

    LegacyAbstractCompactionStrategy get(UUID id)
    {
        return strategies.get(id);
    }

    LegacyAbstractCompactionStrategy get(SSTableReader sstable)
    {
        assert sstable.isPendingRepair();
        return get(sstable.getPendingRepair());
    }

    LegacyAbstractCompactionStrategy getOrCreate(UUID id)
    {
        checkPendingID(id);
        assert id != null;
        LegacyAbstractCompactionStrategy strategy = get(id);
        if (strategy == null)
        {
            synchronized (this)
            {
                strategy = get(id);

                if (strategy == null)
                {
                    logger.debug("Creating {}.{} compaction strategy for pending repair: {}", cfs.metadata.keyspace, cfs.metadata.name, id);
                    strategy = strategyFactory.createLegacyStrategy(params);
                    strategies = mapBuilder().putAll(strategies).put(id, strategy).build();
                }
            }
        }
        return strategy;
    }

    private static void checkPendingID(UUID pendingID)
    {
        if (pendingID == null)
        {
            throw new IllegalSSTableArgumentException("sstable is not pending repair");
        }
    }

    LegacyAbstractCompactionStrategy getOrCreate(SSTableReader sstable)
    {
        return getOrCreate(sstable.getPendingRepair());
    }

    synchronized void removeSessionIfEmpty(UUID sessionID)
    {
        if (!strategies.containsKey(sessionID) || !strategies.get(sessionID).getSSTables().isEmpty())
            return;

        logger.debug("Removing compaction strategy for pending repair {} on  {}.{}", sessionID, cfs.metadata.keyspace, cfs.metadata.name);
        strategies = ImmutableMap.copyOf(Maps.filterKeys(strategies, k -> !k.equals(sessionID)));
    }

    synchronized void removeSSTable(SSTableReader sstable)
    {
        for (Map.Entry<UUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
        {
            entry.getValue().removeSSTable(sstable);
            removeSessionIfEmpty(entry.getKey());
        }
    }

    void removeSSTables(Iterable<SSTableReader> removed)
    {
        for (SSTableReader sstable : removed)
            removeSSTable(sstable);
    }

    synchronized void addSSTable(SSTableReader sstable)
    {
        Preconditions.checkArgument(sstable.isTransient() == isTransient);
        getOrCreate(sstable).addSSTable(sstable);
    }

    void addSSTables(Iterable<SSTableReader> added)
    {
        for (SSTableReader sstable : added)
            addSSTable(sstable);
    }

    synchronized void replaceSSTables(Set<SSTableReader> removed, Set<SSTableReader> added)
    {
        if (removed.isEmpty() && added.isEmpty())
            return;

        // left=removed, right=added
        Map<UUID, Pair<Set<SSTableReader>, Set<SSTableReader>>> groups = new HashMap<>();
        for (SSTableReader sstable : removed)
        {
            UUID sessionID = sstable.getPendingRepair();
            if (!groups.containsKey(sessionID))
            {
                groups.put(sessionID, Pair.create(new HashSet<>(), new HashSet<>()));
            }
            groups.get(sessionID).left.add(sstable);
        }

        for (SSTableReader sstable : added)
        {
            UUID sessionID = sstable.getPendingRepair();
            if (!groups.containsKey(sessionID))
            {
                groups.put(sessionID, Pair.create(new HashSet<>(), new HashSet<>()));
            }
            groups.get(sessionID).right.add(sstable);
        }

        for (Map.Entry<UUID, Pair<Set<SSTableReader>, Set<SSTableReader>>> entry : groups.entrySet())
        {
            LegacyAbstractCompactionStrategy strategy = getOrCreate(entry.getKey());
            Set<SSTableReader> groupRemoved = entry.getValue().left;
            Set<SSTableReader> groupAdded = entry.getValue().right;

            if (!groupRemoved.isEmpty())
                strategy.replaceSSTables(groupRemoved, groupAdded);
            else
                strategy.addSSTables(groupAdded);

            removeSessionIfEmpty(entry.getKey());
        }
    }

    synchronized void startup()
    {
        strategies.values().forEach(CompactionStrategy::startup);
    }

    synchronized void shutdown()
    {
        strategies.values().forEach(CompactionStrategy::shutdown);
    }

    private int getEstimatedRemainingTasks(UUID sessionID, CompactionStrategy strategy)
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
        for (Map.Entry<UUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
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
        for (Map.Entry<UUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
        {
            tasks = Math.max(tasks, getEstimatedRemainingTasks(entry.getKey(), entry.getValue()));
        }
        return tasks;
    }

    @SuppressWarnings("resource")
    private RepairFinishedCompactionTask getRepairFinishedCompactionTask(UUID sessionID)
    {
        Preconditions.checkState(canCleanup(sessionID));
        LegacyAbstractCompactionStrategy compactionStrategy = get(sessionID);
        if (compactionStrategy == null)
            return null;
        Set<SSTableReader> sstables = compactionStrategy.getSSTables();
        long repairedAt = ActiveRepairService.instance.consistent.local.getFinalSessionRepairedAt(sessionID);
        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        return txn == null ? null : new RepairFinishedCompactionTask(cfs, txn, sessionID, repairedAt, isTransient);
    }

    public CleanupTask releaseSessionData(Collection<UUID> sessionIDs)
    {
        List<Pair<UUID, RepairFinishedCompactionTask>> tasks = new ArrayList<>(sessionIDs.size());
        for (UUID session : sessionIDs)
        {
            if (hasDataForSession(session))
            {
                tasks.add(Pair.create(session, getRepairFinishedCompactionTask(session)));
            }
        }
        return new CleanupTask(cfs, tasks);
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

    synchronized Collection<AbstractCompactionTask> getNextRepairFinishedTasks()
    {
        for (UUID sessionID : strategies.keySet())
        {
            if (canCleanup(sessionID))
            {
                RepairFinishedCompactionTask task = getRepairFinishedCompactionTask(sessionID);
                if (task != null)
                    return ImmutableList.of(task);
                else
                    return ImmutableList.of();
            }
        }
        return ImmutableList.of();
    }

    synchronized Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
    {
        if (strategies.isEmpty())
            return ImmutableList.of();

        Map<UUID, Integer> numTasks = new HashMap<>(strategies.size());
        ArrayList<UUID> sessions = new ArrayList<>(strategies.size());
        for (Map.Entry<UUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
        {
            if (canCleanup(entry.getKey()))
            {
                continue;
            }
            numTasks.put(entry.getKey(), getEstimatedRemainingTasks(entry.getKey(), entry.getValue()));
            sessions.add(entry.getKey());
        }

        if (sessions.isEmpty())
            return ImmutableList.of();

        // we want the session with the most compactions at the head of the list
        sessions.sort((o1, o2) -> numTasks.get(o2) - numTasks.get(o1));

        UUID sessionID = sessions.get(0);
        return get(sessionID).getNextBackgroundTasks(gcBefore);
    }

    synchronized Collection<AbstractCompactionTask> getMaximalTasks(int gcBefore, boolean splitOutput)
    {
        if (strategies.isEmpty())
            return ImmutableList.of();

        List<AbstractCompactionTask> maximalTasks = new ArrayList<>(strategies.size());
        for (Map.Entry<UUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
        {
            if (canCleanup(entry.getKey()))
            {
                maximalTasks.add(getRepairFinishedCompactionTask(entry.getKey()));
            }
            else
            {
                maximalTasks.addAll(entry.getValue().getMaximalTasks(gcBefore, splitOutput));
            }
        }
        return maximalTasks;
    }

    Collection<LegacyAbstractCompactionStrategy> getStrategies()
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
            UUID sessionID = sstable.getPendingRepair();
            checkPendingID(sessionID);
            sessionSSTables.computeIfAbsent(sessionID, k -> new HashSet<>()).add(sstable);
        }

        Set<ISSTableScanner> scanners = new HashSet<>(sessionSSTables.size());
        try
        {
            for (Map.Entry<UUID, Set<SSTableReader>> entry : sessionSSTables.entrySet())
            {
                scanners.addAll(getOrCreate(entry.getKey()).getScanners(entry.getValue(), ranges).scanners);
            }
        }
        catch (Throwable t)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, t);
        }
        return scanners;
    }

    public boolean hasStrategy(CompactionStrategy strategy)
    {
        return strategies.values().contains(strategy);
    }

    public synchronized boolean hasDataForSession(UUID sessionID)
    {
        return strategies.containsKey(sessionID);
    }

    boolean containsSSTable(SSTableReader sstable)
    {
        if (!sstable.isPendingRepair())
            return false;

        AbstractCompactionStrategy strategy = strategies.get(sstable.getPendingRepair());
        return strategy != null && strategy.getSSTables().contains(sstable);
    }

    public Collection<AbstractCompactionTask> createUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
    {
        Map<UUID, List<SSTableReader>> group = sstables.stream().collect(Collectors.groupingBy(s -> s.getSSTableMetadata().pendingRepair));
        return group.entrySet().stream().map(g -> strategies.get(g.getKey()).getUserDefinedTasks(g.getValue(), gcBefore)).flatMap(Collection::stream).collect(Collectors.toList());
    }
}
