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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

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
import org.apache.cassandra.utils.TimeUUID;

/**
 * Wrapper that's aware of how sstables are divided between separate strategies,
 * and provides a standard interface to them
 *
 * not threadsafe, calls must be synchronized by caller
 */
public abstract class AbstractStrategyHolder
{
    public static class TaskSupplier implements Comparable<TaskSupplier>
    {
        private final int numRemaining;
        private final Supplier<AbstractCompactionTask> supplier;

        TaskSupplier(int numRemaining, Supplier<AbstractCompactionTask> supplier)
        {
            this.numRemaining = numRemaining;
            this.supplier = supplier;
        }

        public AbstractCompactionTask getTask()
        {
            return supplier.get();
        }

        public int compareTo(TaskSupplier o)
        {
            return o.numRemaining - numRemaining;
        }
    }

    public static interface DestinationRouter
    {
        int getIndexForSSTable(SSTableReader sstable);
        int getIndexForSSTableDirectory(Descriptor descriptor);
    }

    /**
     * Maps sstables to their token partition bucket
     */
    public static class GroupedSSTableContainer
    {
        private final AbstractStrategyHolder holder;
        private final Set<SSTableReader>[] groups;

        private GroupedSSTableContainer(AbstractStrategyHolder holder)
        {
            this.holder = holder;
            Preconditions.checkArgument(holder.numTokenPartitions > 0, "numTokenPartitions not set");
            groups = new Set[holder.numTokenPartitions];
        }

        void add(SSTableReader sstable)
        {
            Preconditions.checkArgument(holder.managesSSTable(sstable), "this strategy holder doesn't manage %s", sstable);
            int idx = holder.router.getIndexForSSTable(sstable);
            Preconditions.checkState(idx >= 0 && idx < holder.numTokenPartitions, "Invalid sstable index (%s) for %s", idx, sstable);
            if (groups[idx] == null)
                groups[idx] = new HashSet<>();
            groups[idx].add(sstable);
        }

        public int numGroups()
        {
            return groups.length;
        }

        public Set<SSTableReader> getGroup(int i)
        {
            Preconditions.checkArgument(i >= 0 && i < groups.length);
            Set<SSTableReader> group = groups[i];
            return group != null ? group : Collections.emptySet();
        }

        boolean isGroupEmpty(int i)
        {
            return getGroup(i).isEmpty();
        }

        boolean isEmpty()
        {
            for (int i = 0; i < groups.length; i++)
                if (!isGroupEmpty(i))
                    return false;
            return true;
        }
    }

    protected final ColumnFamilyStore cfs;
    final DestinationRouter router;
    private int numTokenPartitions = -1;

    AbstractStrategyHolder(ColumnFamilyStore cfs, DestinationRouter router)
    {
        this.cfs = cfs;
        this.router = router;
    }

    public abstract void startup();

    public abstract void shutdown();

    final void setStrategy(CompactionParams params, int numTokenPartitions)
    {
        Preconditions.checkArgument(numTokenPartitions > 0, "at least one token partition required");
        shutdown();
        this.numTokenPartitions = numTokenPartitions;
        setStrategyInternal(params, numTokenPartitions);
    }

    protected abstract void setStrategyInternal(CompactionParams params, int numTokenPartitions);

    /**
     * SSTables are grouped by their repaired and pending repair status. This method determines if this holder
     * holds the sstable for the given repaired/grouped statuses. Holders should be mutually exclusive in the
     * groups they deal with. IOW, if one holder returns true for a given isRepaired/isPendingRepair combo,
     * none of the others should.
     */
    public abstract boolean managesRepairedGroup(boolean isRepaired, boolean isPendingRepair, boolean isTransient);

    public boolean managesSSTable(SSTableReader sstable)
    {
        return managesRepairedGroup(sstable.isRepaired(), sstable.isPendingRepair(), sstable.isTransient());
    }

    public abstract AbstractCompactionStrategy getStrategyFor(SSTableReader sstable);

    public abstract Iterable<AbstractCompactionStrategy> allStrategies();

    public abstract Collection<TaskSupplier> getBackgroundTaskSuppliers(long gcBefore);

    public abstract Collection<AbstractCompactionTask> getMaximalTasks(long gcBefore, boolean splitOutput);

    public abstract Collection<AbstractCompactionTask> getUserDefinedTasks(GroupedSSTableContainer sstables, long gcBefore);

    public GroupedSSTableContainer createGroupedSSTableContainer()
    {
        return new GroupedSSTableContainer(this);
    }

    public abstract void addSSTables(GroupedSSTableContainer sstables);

    public abstract void removeSSTables(GroupedSSTableContainer sstables);

    public abstract void replaceSSTables(GroupedSSTableContainer removed, GroupedSSTableContainer added);

    public abstract List<ISSTableScanner> getScanners(GroupedSSTableContainer sstables, Collection<Range<Token>> ranges);


    public abstract SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                                long keyCount,
                                                                long repairedAt,
                                                                TimeUUID pendingRepair,
                                                                boolean isTransient,
                                                                IntervalSet<CommitLogPosition> commitLogPositions,
                                                                int sstableLevel,
                                                                SerializationHeader header,
                                                                Collection<Index.Group> indexGroups,
                                                                LifecycleNewTracker lifecycleNewTracker);

    /**
     * Return the directory index the given compaction strategy belongs to, or -1
     * if it's not held by this holder
     */
    public abstract int getStrategyIndex(AbstractCompactionStrategy strategy);

    public abstract boolean containsSSTable(SSTableReader sstable);

    public abstract int getEstimatedRemainingTasks();
}
