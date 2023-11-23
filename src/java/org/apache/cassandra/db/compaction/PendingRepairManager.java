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
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import org.apache.cassandra.repair.consistent.admin.CleanupSummary;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;

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
    private final boolean isTransient;
    private volatile ImmutableMap<TimeUUID, AbstractCompactionStrategy> strategies = ImmutableMap.of();

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

    PendingRepairManager(ColumnFamilyStore cfs, CompactionParams params, boolean isTransient)
    {
        this.cfs = cfs;
        this.params = params;
        this.isTransient = isTransient;
    }

    private ImmutableMap.Builder<TimeUUID, AbstractCompactionStrategy> mapBuilder()
    {
        return ImmutableMap.builder();
    }

    AbstractCompactionStrategy get(TimeUUID id)
    {
        return strategies.get(id);
    }

    AbstractCompactionStrategy get(SSTableReader sstable)
    {
        assert sstable.isPendingRepair();
        return get(sstable.getSSTableMetadata().pendingRepair);
    }

    AbstractCompactionStrategy getOrCreate(TimeUUID id)
    {
        checkPendingID(id);
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

    private static void checkPendingID(TimeUUID pendingID)
    {
        if (pendingID == null)
        {
            throw new IllegalSSTableArgumentException("sstable is not pending repair");
        }
    }

    AbstractCompactionStrategy getOrCreate(SSTableReader sstable)
    {
        return getOrCreate(sstable.getSSTableMetadata().pendingRepair);
    }

    private synchronized void removeSessionIfEmpty(TimeUUID sessionID)
    {
        if (!strategies.containsKey(sessionID) || !strategies.get(sessionID).getSSTables().isEmpty())
            return;

        logger.debug("Removing compaction strategy for pending repair {} on  {}.{}", sessionID, cfs.metadata.keyspace, cfs.metadata.name);
        strategies = ImmutableMap.copyOf(Maps.filterKeys(strategies, k -> !k.equals(sessionID)));
    }

    synchronized void removeSSTable(SSTableReader sstable)
    {
        for (Map.Entry<TimeUUID, AbstractCompactionStrategy> entry : strategies.entrySet())
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
        Map<TimeUUID, Pair<Set<SSTableReader>, Set<SSTableReader>>> groups = new HashMap<>();
        for (SSTableReader sstable : removed)
        {
            TimeUUID sessionID = sstable.getSSTableMetadata().pendingRepair;
            if (!groups.containsKey(sessionID))
            {
                groups.put(sessionID, Pair.create(new HashSet<>(), new HashSet<>()));
            }
            groups.get(sessionID).left.add(sstable);
        }

        for (SSTableReader sstable : added)
        {
            TimeUUID sessionID = sstable.getSSTableMetadata().pendingRepair;
            if (!groups.containsKey(sessionID))
            {
                groups.put(sessionID, Pair.create(new HashSet<>(), new HashSet<>()));
            }
            groups.get(sessionID).right.add(sstable);
        }

        for (Map.Entry<TimeUUID, Pair<Set<SSTableReader>, Set<SSTableReader>>> entry : groups.entrySet())
        {
            AbstractCompactionStrategy strategy = getOrCreate(entry.getKey());
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
        strategies.values().forEach(AbstractCompactionStrategy::startup);
    }

    synchronized void shutdown()
    {
        strategies.values().forEach(AbstractCompactionStrategy::shutdown);
    }

    private int getEstimatedRemainingTasks(TimeUUID sessionID, AbstractCompactionStrategy strategy)
    {
        return getEstimatedRemainingTasks(sessionID, strategy, 0, 0);
    }

    private int getEstimatedRemainingTasks(TimeUUID sessionID, AbstractCompactionStrategy strategy, int additionalSSTables, long additionalBytes)
    {
        return canCleanup(sessionID) ? 0 : strategy.getEstimatedRemainingTasks();
    }

    int getEstimatedRemainingTasks()
    {
        return getEstimatedRemainingTasks(0, 0);
    }

    int getEstimatedRemainingTasks(int additionalSSTables, long additionalBytes)
    {
        int tasks = 0;
        for (Map.Entry<TimeUUID, AbstractCompactionStrategy> entry : strategies.entrySet())
        {
            tasks += getEstimatedRemainingTasks(entry.getKey(), entry.getValue(), additionalSSTables, additionalBytes);
        }
        return tasks;
    }

    /**
     * @return the highest max remaining tasks of all contained compaction strategies
     */
    int getMaxEstimatedRemainingTasks()
    {
        int tasks = 0;
        for (Map.Entry<TimeUUID, AbstractCompactionStrategy> entry : strategies.entrySet())
        {
            tasks = Math.max(tasks, getEstimatedRemainingTasks(entry.getKey(), entry.getValue()));
        }
        return tasks;
    }

    private RepairFinishedCompactionTask getRepairFinishedCompactionTask(TimeUUID sessionID)
    {
        Preconditions.checkState(canCleanup(sessionID));
        AbstractCompactionStrategy compactionStrategy = get(sessionID);
        if (compactionStrategy == null)
            return null;
        Set<SSTableReader> sstables = compactionStrategy.getSSTables();
        long repairedAt = ActiveRepairService.instance().consistent.local.getFinalSessionRepairedAt(sessionID);
        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        return txn == null ? null : new RepairFinishedCompactionTask(cfs, txn, sessionID, repairedAt);
    }

    public static class CleanupTask
    {
        private final ColumnFamilyStore cfs;
        private final List<Pair<TimeUUID, RepairFinishedCompactionTask>> tasks;

        public CleanupTask(ColumnFamilyStore cfs, List<Pair<TimeUUID, RepairFinishedCompactionTask>> tasks)
        {
            this.cfs = cfs;
            this.tasks = tasks;
        }

        public CleanupSummary cleanup()
        {
            Set<TimeUUID> successful = new HashSet<>();
            Set<TimeUUID> unsuccessful = new HashSet<>();
            for (Pair<TimeUUID, RepairFinishedCompactionTask> pair : tasks)
            {
                TimeUUID session = pair.left;
                RepairFinishedCompactionTask task = pair.right;

                if (task != null)
                {
                    try
                    {
                        task.run();
                        successful.add(session);
                    }
                    catch (Throwable t)
                    {
                        t = task.transaction.abort(t);
                        logger.error("Failed cleaning up " + session, t);
                        unsuccessful.add(session);
                    }
                }
                else
                {
                    unsuccessful.add(session);
                }
            }
            return new CleanupSummary(cfs, successful, unsuccessful);
        }

        public Throwable abort(Throwable accumulate)
        {
            for (Pair<TimeUUID, RepairFinishedCompactionTask> pair : tasks)
                accumulate = pair.right.transaction.abort(accumulate);
            return accumulate;
        }
    }

    public CleanupTask releaseSessionData(Collection<TimeUUID> sessionIDs)
    {
        List<Pair<TimeUUID, RepairFinishedCompactionTask>> tasks = new ArrayList<>(sessionIDs.size());
        for (TimeUUID session : sessionIDs)
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
        for (TimeUUID sessionID : strategies.keySet())
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
        for (TimeUUID sessionID : strategies.keySet())
        {
            if (canCleanup(sessionID))
            {
                return getRepairFinishedCompactionTask(sessionID);
            }
        }
        return null;
    }

    synchronized AbstractCompactionTask getNextBackgroundTask(long gcBefore)
    {
        if (strategies.isEmpty())
            return null;

        Map<TimeUUID, Integer> numTasks = new HashMap<>(strategies.size());
        ArrayList<TimeUUID> sessions = new ArrayList<>(strategies.size());
        for (Map.Entry<TimeUUID, AbstractCompactionStrategy> entry : strategies.entrySet())
        {
            if (canCleanup(entry.getKey()))
            {
                continue;
            }
            numTasks.put(entry.getKey(), getEstimatedRemainingTasks(entry.getKey(), entry.getValue()));
            sessions.add(entry.getKey());
        }

        if (sessions.isEmpty())
            return null;

        // we want the session with the most compactions at the head of the list
        sessions.sort((o1, o2) -> numTasks.get(o2) - numTasks.get(o1));

        TimeUUID sessionID = sessions.get(0);
        return get(sessionID).getNextBackgroundTask(gcBefore);
    }

    synchronized Collection<AbstractCompactionTask> getMaximalTasks(long gcBefore, boolean splitOutput)
    {
        if (strategies.isEmpty())
            return null;

        List<AbstractCompactionTask> maximalTasks = new ArrayList<>(strategies.size());
        for (Map.Entry<TimeUUID, AbstractCompactionStrategy> entry : strategies.entrySet())
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

    Set<TimeUUID> getSessions()
    {
        return strategies.keySet();
    }

    boolean canCleanup(TimeUUID sessionID)
    {
        return !ActiveRepairService.instance().consistent.local.isSessionInProgress(sessionID);
    }

    synchronized Set<ISSTableScanner> getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        if (sstables.isEmpty())
        {
            return Collections.emptySet();
        }

        Map<TimeUUID, Set<SSTableReader>> sessionSSTables = new HashMap<>();
        for (SSTableReader sstable : sstables)
        {
            TimeUUID sessionID = sstable.getSSTableMetadata().pendingRepair;
            checkPendingID(sessionID);
            sessionSSTables.computeIfAbsent(sessionID, k -> new HashSet<>()).add(sstable);
        }

        Set<ISSTableScanner> scanners = new HashSet<>(sessionSSTables.size());
        try
        {
            for (Map.Entry<TimeUUID, Set<SSTableReader>> entry : sessionSSTables.entrySet())
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

    public boolean hasStrategy(AbstractCompactionStrategy strategy)
    {
        return strategies.values().contains(strategy);
    }

    public synchronized boolean hasDataForSession(TimeUUID sessionID)
    {
        return strategies.keySet().contains(sessionID);
    }

    boolean containsSSTable(SSTableReader sstable)
    {
        if (!sstable.isPendingRepair())
            return false;

        AbstractCompactionStrategy strategy = strategies.get(sstable.getPendingRepair());
        return strategy != null && strategy.getSSTables().contains(sstable);
    }

    public Collection<AbstractCompactionTask> createUserDefinedTasks(Collection<SSTableReader> sstables, long gcBefore)
    {
        Map<TimeUUID, List<SSTableReader>> group = sstables.stream().collect(Collectors.groupingBy(s -> s.getSSTableMetadata().pendingRepair));
        return group.entrySet().stream().map(g -> strategies.get(g.getKey()).getUserDefinedTask(g.getValue(), gcBefore)).collect(Collectors.toList());
    }

    /**
     * promotes/demotes sstables involved in a consistent repair that has been finalized, or failed
     */
    class RepairFinishedCompactionTask extends AbstractCompactionTask
    {
        private final TimeUUID sessionID;
        private final long repairedAt;

        RepairFinishedCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction transaction, TimeUUID sessionID, long repairedAt)
        {
            super(cfs, transaction);
            this.sessionID = sessionID;
            this.repairedAt = repairedAt;
        }

        @VisibleForTesting
        TimeUUID getSessionID()
        {
            return sessionID;
        }

        protected void runMayThrow() throws Exception
        {
            boolean completed = false;
            boolean obsoleteSSTables = isTransient && repairedAt > 0;
            try
            {
                if (obsoleteSSTables)
                {
                    logger.info("Obsoleting transient repaired sstables for {}", sessionID);
                    Preconditions.checkState(Iterables.all(transaction.originals(), SSTableReader::isTransient));
                    transaction.obsoleteOriginals();
                }
                else
                {
                    logger.info("Moving {} from pending to repaired with repaired at = {} and session id = {}", transaction.originals(), repairedAt, sessionID);
                    cfs.getCompactionStrategyManager().mutateRepaired(transaction.originals(), repairedAt, ActiveRepairService.NO_PENDING_REPAIR, false);
                }
                completed = true;
            }
            finally
            {
                if (obsoleteSSTables)
                {
                    transaction.finish();
                }
                else
                {
                    // we abort here because mutating metadata isn't guarded by LifecycleTransaction, so this won't roll
                    // anything back. Also, we don't want to obsolete the originals. We're only using it to prevent other
                    // compactions from marking these sstables compacting, and unmarking them when we're done
                    transaction.abort();
                }
                if (completed)
                {
                    removeSessionIfEmpty(sessionID);
                }
            }
        }

        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables)
        {
            throw new UnsupportedOperationException();
        }

        protected int executeInternal(ActiveCompactionsTracker activeCompactions)
        {
            run();
            return transaction.originals().size();
        }
    }

}
