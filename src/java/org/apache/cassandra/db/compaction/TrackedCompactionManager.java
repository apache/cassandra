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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.replication.ImmutableCoordinatorLogOffsets;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Holds {@link CompactionGroup#UNRECONCILED} sstables, isolating them into one silo per set of activated transfer
 * ids, and promoting the contents of each silo to repaired as its contents are durably reconciled - continuously in
 * the case of normal writes, all at once in the case of tracked transfers. This is the mutation tracking counterpart
 * to {@link PendingRepairManager}. SSTables created by the normal write path don't have transfer ids and are in the
 * silo under the key {@link #NONE}. Like {@link PendingRepairManager}, silos are created lazily per key, and discarded
 * when empty, with the exception of the {@link #NONE} silo, which is never purged.
 */
public class TrackedCompactionManager extends AbstractStrategyHolder
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedCompactionManager.class);

    /**
     * Silo for tracked sstables that carry mutation offsets and no activated transfers. Created with the strategy and
     * never pruned, because ordinary tracked flushes land here continuously.
     *
     * {@link ImmutableCoordinatorLogOffsets#transferSiloKey()} is empty for exactly these sstables, so routing one
     * needs no separate classification.
     */
    static final ImmutableSet<ShortMutationId> NONE = ImmutableSet.of();

    private CompactionParams params;
    private int numTokenPartitions;

    private volatile ImmutableMap<ImmutableSet<ShortMutationId>, CompactionStrategyHolder> silos = ImmutableMap.of();

    public TrackedCompactionManager(ColumnFamilyStore cfs, DestinationRouter router)
    {
        super(cfs, router);
    }

    static ImmutableSet<ShortMutationId> keyOf(SSTableReader sstable)
    {
        return sstable.getSSTableMetadata().coordinatorLogOffsets.transferSiloKey();
    }

    private static String describe(ImmutableSet<ShortMutationId> key)
    {
        return key.isEmpty() ? "reconciled mutations" : "tracked transfers " + key;
    }

    @Override
    public boolean managesGroup(CompactionGroup group)
    {
        return group == CompactionGroup.UNRECONCILED;
    }

    @Override
    public void setStrategyInternal(CompactionParams params, int numTokenPartitions)
    {
        this.params = params;
        this.numTokenPartitions = numTokenPartitions;
        this.silos = ImmutableMap.of(NONE, newSilo(NONE));
    }

    private CompactionStrategyHolder newSilo(ImmutableSet<ShortMutationId> key)
    {
        logger.debug("Creating {}.{} compaction strategies for tracked transfers: {}",
                     cfs.metadata.keyspace, cfs.metadata.name, key);
        CompactionStrategyHolder silo = new CompactionStrategyHolder(cfs, router, CompactionGroup.UNRECONCILED);
        silo.setStrategy(params, numTokenPartitions);
        return silo;
    }

    CompactionStrategyHolder getIfPresent(ImmutableSet<ShortMutationId> key)
    {
        return silos.get(key);
    }

    CompactionStrategyHolder getIfPresent(SSTableReader sstable)
    {
        return getIfPresent(keyOf(sstable));
    }

    CompactionStrategyHolder getOrCreate(ImmutableSet<ShortMutationId> key)
    {
        CompactionStrategyHolder silo = silos.get(key);
        if (silo == null)
        {
            synchronized (this)
            {
                silo = silos.get(key);
                if (silo == null)
                {
                    silo = newSilo(key);
                    silos = ImmutableMap.<ImmutableSet<ShortMutationId>, CompactionStrategyHolder>builder()
                                        .putAll(silos).put(key, silo).build();
                }
            }
        }
        return silo;
    }

    CompactionStrategyHolder getOrCreate(SSTableReader sstable)
    {
        return getOrCreate(keyOf(sstable));
    }

    private static Iterable<SSTableReader> sstablesIn(CompactionStrategyHolder silo)
    {
        return Iterables.concat(Iterables.transform(silo.allStrategies(), AbstractCompactionStrategy::getSSTables));
    }

    private static boolean isEmpty(CompactionStrategyHolder silo)
    {
        return Iterables.isEmpty(sstablesIn(silo));
    }

    /**
     * Drop every transfer silo that holds no sstables. Called from the paths that walk the map anyway, so teardown does
     * not depend on any particular removal notification arriving. The silo that holds normal writes is never pruned
     *
     * @return true if anything was dropped
     */
    synchronized boolean pruneEmpty()
    {
        Set<ImmutableSet<ShortMutationId>> empty = null;
        for (Map.Entry<ImmutableSet<ShortMutationId>, CompactionStrategyHolder> entry : silos.entrySet())
        {
            // don't prune the normal write silo
            if (entry.getKey().isEmpty())
                continue;

            if (isEmpty(entry.getValue()))
            {
                if (empty == null)
                    empty = new HashSet<>();
                empty.add(entry.getKey());
            }
        }

        if (empty == null)
            return false;

        Set<ImmutableSet<ShortMutationId>> dropped = empty;
        logger.debug("Removing {}.{} compaction strategies for reconciled or emptied tracked transfers: {}",
                     cfs.metadata.keyspace, cfs.metadata.name, dropped);
        for (ImmutableSet<ShortMutationId> key : dropped)
            silos.get(key).shutdown();
        silos = ImmutableMap.copyOf(Maps.filterKeys(silos, k -> !dropped.contains(k)));
        return true;
    }

    @Override
    public synchronized void startup()
    {
        silos.values().forEach(CompactionStrategyHolder::startup);
    }

    @Override
    public synchronized void shutdown()
    {
        silos.values().forEach(CompactionStrategyHolder::shutdown);
    }

    @Override
    public AbstractCompactionStrategy getStrategyFor(SSTableReader sstable)
    {
        Preconditions.checkArgument(managesSSTable(sstable), "Attempting to get compaction strategy from wrong holder");
        return getOrCreate(sstable).getStrategyFor(sstable);
    }

    @Override
    public Iterable<AbstractCompactionStrategy> allStrategies()
    {
        return Iterables.concat(Iterables.transform(silos.values(), AbstractStrategyHolder::allStrategies));
    }

    @Override
    public synchronized void addSSTable(SSTableReader sstable)
    {
        Preconditions.checkArgument(managesSSTable(sstable), "Attempting to add sstable from wrong holder");
        getOrCreate(sstable).addSSTable(sstable);
    }

    @VisibleForTesting
    synchronized void addSSTables(Iterable<SSTableReader> sstables)
    {
        for (SSTableReader sstable : sstables)
            addSSTable(sstable);
    }

    @Override
    public synchronized void addSSTables(GroupedSSTableContainer sstables)
    {
        for (Map.Entry<ImmutableSet<ShortMutationId>, GroupedSSTableContainer> entry : splitByKey(sstables).entrySet())
            getOrCreate(entry.getKey()).addSSTables(entry.getValue());
    }

    @Override
    public synchronized void removeSSTables(GroupedSSTableContainer sstables)
    {
        for (CompactionStrategyHolder silo : silos.values())
            silo.removeSSTables(sstables);
        pruneEmpty();
    }

    @VisibleForTesting
    synchronized void removeSSTable(SSTableReader sstable)
    {
        for (CompactionStrategyHolder silo : silos.values())
            silo.getStrategyFor(sstable).removeSSTable(sstable);
        pruneEmpty();
    }

    @Override
    public synchronized void replaceSSTables(GroupedSSTableContainer removed, GroupedSSTableContainer added)
    {
        Map<ImmutableSet<ShortMutationId>, GroupedSSTableContainer> addedByKey = splitByKey(added);

        // Removals go to every silo, for the reason given on removeSSTables.
        for (Map.Entry<ImmutableSet<ShortMutationId>, CompactionStrategyHolder> entry : silos.entrySet())
        {
            CompactionStrategyHolder silo = entry.getValue();
            GroupedSSTableContainer addedForSilo = addedByKey.get(entry.getKey());
            silo.replaceSSTables(removed, addedForSilo == null ? silo.createGroupedSSTableContainer() : addedForSilo);
        }
        pruneEmpty();
    }

    /**
     * Regroups a container by silo key, creating any silo it does not find
     */
    private Map<ImmutableSet<ShortMutationId>, GroupedSSTableContainer> splitByKey(GroupedSSTableContainer sstables)
    {
        Map<ImmutableSet<ShortMutationId>, GroupedSSTableContainer> split = new HashMap<>();
        for (int i = 0; i < sstables.numGroups(); i++)
        {
            for (SSTableReader sstable : sstables.getGroup(i))
            {
                ImmutableSet<ShortMutationId> key = keyOf(sstable);
                split.computeIfAbsent(key, k -> getOrCreate(k).createGroupedSSTableContainer()).add(sstable);
            }
        }
        return split;
    }

    @Override
    public synchronized List<ISSTableScanner> getScanners(GroupedSSTableContainer sstables, Collection<Range<Token>> ranges)
    {
        List<ISSTableScanner> scanners = new ArrayList<>();
        try
        {
            for (Map.Entry<ImmutableSet<ShortMutationId>, GroupedSSTableContainer> entry : splitByKey(sstables).entrySet())
                scanners.addAll(getOrCreate(entry.getKey()).getScanners(entry.getValue(), ranges));
        }
        catch (Throwable t)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, t);
        }
        return scanners;
    }

    @Override
    public synchronized Collection<AbstractCompactionTask> getUserDefinedTasks(GroupedSSTableContainer sstables, long gcBefore)
    {
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (Map.Entry<ImmutableSet<ShortMutationId>, GroupedSSTableContainer> entry : splitByKey(sstables).entrySet())
        {
            // CompactionStrategyHolder passes through whatever getUserDefinedTask returns, including null.
            for (AbstractCompactionTask task : getOrCreate(entry.getKey()).getUserDefinedTasks(entry.getValue(), gcBefore))
            {
                if (task != null)
                    tasks.add(task);
            }
        }
        return tasks;
    }

    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                      long keyCount,
                                                      long repairedAt,
                                                      TimeUUID pendingRepair,
                                                      ImmutableCoordinatorLogOffsets coordinatorLogOffsets,
                                                      IntervalSet<CommitLogPosition> commitLogPositions,
                                                      int sstableLevel,
                                                      SerializationHeader header,
                                                      Collection<Index.Group> indexGroups,
                                                      ILifecycleTransaction txn)
    {
        // These guards read pre-write metadata. SSTableWriter.finalizeMetadata() can promote the sstable as it is
        // written, so the holder that creates the writer and the holder that ends up with the sstable can differ.
        Preconditions.checkArgument(repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE,
                                    "TrackedCompactionManager can't create sstable writer with repaired at set");
        Preconditions.checkArgument(pendingRepair == ActiveRepairService.NO_PENDING_REPAIR,
                                    "TrackedCompactionManager can't create sstable writer with pendingRepair id");

        // The silo indexes by write destination rather than by token, so this creates no strategy in the wrong place.
        return getOrCreate(coordinatorLogOffsets.transferSiloKey())
               .createSSTableMultiWriter(descriptor,
                                         keyCount,
                                         repairedAt,
                                         pendingRepair,
                                         coordinatorLogOffsets,
                                         commitLogPositions,
                                         sstableLevel,
                                         header,
                                         indexGroups,
                                         txn);
    }

    @Override
    public int getStrategyIndex(AbstractCompactionStrategy strategy)
    {
        for (CompactionStrategyHolder silo : silos.values())
        {
            int idx = silo.getStrategyIndex(strategy);
            if (idx >= 0)
                return idx;
        }
        return -1;
    }

    @Override
    public boolean containsSSTable(SSTableReader sstable)
    {
        return Iterables.any(silos.values(), silo -> silo.containsSSTable(sstable));
    }

    @VisibleForTesting
    boolean isPromotable(SSTableReader sstable)
    {
        if (!MutationTrackingService.instance().isStarted())
            return false;

        if (sstable.isRepaired() || sstable.isPendingRepair())
            return false;

        ImmutableCoordinatorLogOffsets offsets = sstable.getSSTableMetadata().coordinatorLogOffsets;
        if (offsets.isEmpty())
            return false;

        return MutationTrackingService.instance().isDurablyReconciled(offsets);
    }

    @VisibleForTesting
    synchronized Set<SSTableReader> promotableSSTables(ImmutableSet<ShortMutationId> key)
    {
        CompactionStrategyHolder silo = silos.get(key);
        if (silo == null)
            return Collections.emptySet();

        Set<SSTableReader> promotable = new HashSet<>();
        for (SSTableReader sstable : sstablesIn(silo))
        {
            if (isPromotable(sstable))
                promotable.add(sstable);
        }
        return promotable;
    }

    @VisibleForTesting
    synchronized Set<SSTableReader> promotableSSTables()
    {
        // TODO: consider grabbing a snapshot of reconciled offsets and comparing against
        //  sstable metadata without allocating collections
        Set<SSTableReader> promotable = new HashSet<>();
        for (ImmutableSet<ShortMutationId> key : silos.keySet())
            promotable.addAll(promotableSSTables(key));
        return promotable;
    }

    @VisibleForTesting
    synchronized Set<SSTableReader> sstablesFor(ImmutableSet<ShortMutationId> key)
    {
        CompactionStrategyHolder silo = silos.get(key);
        return silo == null ? Collections.emptySet() : ImmutableSet.copyOf(sstablesIn(silo));
    }

    @Override
    public synchronized int getEstimatedRemainingTasks()
    {
        pruneEmpty();
        int tasks = 0;
        for (CompactionStrategyHolder silo : silos.values())
            tasks += silo.getEstimatedRemainingTasks();
        return tasks;
    }

    /**
     * One supplier per strategy, from the silos not awaiting promotion.
     *
     * The number each supplier carries is its own strategy's estimate, because that is what its callback compacts.
     * {@link CompactionStrategyManager#getNextBackgroundTasks} sorts every holder's suppliers together, so a per silo
     * or per manager total would let tracked compaction outrank a busier strategy elsewhere.
     */
    @Override
    public synchronized Collection<TaskSupplier> getBackgroundTaskSuppliers(long gcBefore)
    {
        pruneEmpty();
        List<TaskSupplier> suppliers = new ArrayList<>();
        for (CompactionStrategyHolder silo : silos.values())
            suppliers.addAll(silo.getBackgroundTaskSuppliers(gcBefore));
        return suppliers;
    }

    @Override
    public synchronized Collection<AbstractCompactionTask> getMaximalTasks(long gcBefore, boolean splitOutput)
    {
        pruneEmpty();

        // Promotion first
        List<AbstractCompactionTask> tasks = new ArrayList<>(getNextPromotionTasks());
        for (CompactionStrategyHolder silo : silos.values())
        {
            Collection<AbstractCompactionTask> siloTasks = silo.getMaximalTasks(gcBefore, splitOutput);
            if (siloTasks != null)
                tasks.addAll(siloTasks);
        }
        return tasks;
    }

    @VisibleForTesting
    synchronized boolean hasDataFor(ImmutableSet<ShortMutationId> key)
    {
        CompactionStrategyHolder silo = silos.get(key);
        return silo != null && !isEmpty(silo);
    }

    @VisibleForTesting
    synchronized Set<ImmutableSet<ShortMutationId>> keys()
    {
        return ImmutableSet.copyOf(silos.keySet());
    }

    synchronized Collection<AbstractCompactionTask> getNextPromotionTasks()
    {
        pruneEmpty();
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (ImmutableSet<ShortMutationId> key : silos.keySet())
        {
            AbstractCompactionTask task = getPromotionTask(key);
            if (task != null)
                tasks.add(task);
        }
        return tasks;
    }

    @VisibleForTesting
    synchronized AbstractCompactionTask getPromotionTask(ImmutableSet<ShortMutationId> key)
    {
        if (silos.get(key) == null)
            return null;

        return PromoteReconciledTask.tryPromote(cfs, promotableSSTables(key), describe(key), this::pruneEmpty);
    }
}
