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

package org.apache.cassandra.service.reads.tracked;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SimpleBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.Future;

public abstract class PartialTrackedRangeRead extends AbstractPartialTrackedRead
{
    private static final Logger logger = LoggerFactory.getLogger(PartialTrackedRangeRead.class);

    protected static class FollowUpReadInfo
    {
        int potentialMatches = 0;
    }

    protected final PartitionRangeReadCommand command;
    protected final SortedMap<DecoratedKey, SimpleBTreePartition> data = new TreeMap<>();
    private final UnfilteredPartitionIterator initialData;
    private final boolean enforceStrictLiveness;

    // short read support
    private DecoratedKey lastPartitionKey; // key of the last observed partition
    protected boolean partitionsFetched; // whether we've seen any new partitions since iteration start or last moreContents() call
    protected boolean initialIteratorExhausted;
    private boolean wasAugmented;
    protected  AbstractBounds<PartitionPosition> followUpBounds;

    PartialTrackedRangeRead(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, PartitionRangeReadCommand command, UnfilteredPartitionIterator initialData)
    {
        super(executionController, cfs, startTimeNanos);
        this.command = command;
        this.initialData = initialData;
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
    }

    public static PartialTrackedRangeRead create(ReadExecutionController executionController, Index.Searcher searcher, ColumnFamilyStore cfs, long startTimeNanos, PartitionRangeReadCommand command, UnfilteredPartitionIterator initialData)
    {
        RowFilter rowFilter = command.rowFilter();
        PartialTrackedRangeRead read;
        if (searcher != null)
        {
            throw new UnsupportedOperationException("TODO: CASSANDRA-20374");
        }
        else if (!rowFilter.isEmpty())
        {
            read = new PartialTrackedRangeRead.Filtered(executionController, cfs, startTimeNanos, command, initialData);
        }
        else
        {
            read = new PartialTrackedRangeRead.Simple(executionController, cfs, startTimeNanos, command, initialData);
        }

        try
        {
            read.prepare();
            return read;
        }
        catch (Throwable e)
        {
            read.close();
            throw e;
        }
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    UnfilteredRowIterator queryPartition(AbstractBTreePartition partition)
    {
        return partition.unfilteredIterator(command.columnFilter(),
                                            command.requestedSlices(),
                                            command.clusteringIndexFilter(partition.partitionKey()).isReversed());
    }

    private static void consume(UnfilteredPartitionIterator iterator)
    {
        while (iterator.hasNext())
        {
            try (UnfilteredRowIterator partition = iterator.next())
            {
                while (partition.hasNext())
                    partition.next();
            }
        }
    }

    protected abstract UnfilteredPartitionIterator filter(UnfilteredPartitionIterator partition);

    @Override
    void freezeInitialData()
    {
        // memtable contents are frozen at read completion time, when the iterator is evaluated, not at the beginning
        // of the read, when references to memtables and sstables are collected. Because of this, replica coordinated
        // reads can cause read monotonicity to be broken by returning data that hasn't been replicated to at least
        // CL other nodes via reconciliation. To prevent this, the contents of the initial iterator are materialized
        // onto heap at partition granularity until the limits of the read are reached.

        UnfilteredPartitionIterator materializer = new AbstractUnfilteredPartitionIterator()
        {
            @Override
            public TableMetadata metadata()
            {
                return initialData.metadata();
            }

            @Override
            public boolean hasNext()
            {
                return initialData.hasNext();
            }

            @Override
            public UnfilteredRowIterator next()
            {
                try (UnfilteredRowIterator rowIterator = initialData.next())
                {
                    SimpleBTreePartition partition = augmentResponseInternal(PartitionUpdate.fromIterator(rowIterator, command.columnFilter()));
                    lastPartitionKey = partition.partitionKey();
                    partitionsFetched = true;
                    return queryPartition(partition);
                }
            }

            @Override
            public void close()
            {
                super.close();
                initialData.close();
            }
        };

        UnfilteredPartitionIterator filtered = filter(materializer);

        // unmerged per-source counter
        final DataLimits.Counter singleResultCounter = command.limits().newCounter(command.nowInSec(),
                                                                                   false,
                                                                                   command.selectsFullPartition(),
                                                                                   enforceStrictLiveness);
        try (UnfilteredPartitionIterator iterator = singleResultCounter.applyTo(filtered))
        {
            consume(iterator);
        }
        initialIteratorExhausted = command.limits().isExhausted(singleResultCounter);
        if (partitionsFetched)
        {
            AbstractBounds<PartitionPosition> bounds = command.dataRange().keyRange();
            followUpBounds = bounds.inclusiveRight()
                             ? new Range<>(lastPartitionKey, bounds.right)
                             : new ExcludingBounds<>(lastPartitionKey, bounds.right);
            Preconditions.checkState(!followUpBounds.contains(lastPartitionKey));
        }
        wasAugmented = false;
    }

    @Override
    UnfilteredPartitionIterator initialData()
    {
        Iterator<SimpleBTreePartition> iterator = data.values().iterator();
        return new AbstractUnfilteredPartitionIterator()
        {
            @Override
            public TableMetadata metadata()
            {
                return command.metadata();
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public UnfilteredRowIterator next()
            {
                return queryPartition(iterator.next());
            }
        };
    }

    @Override
    UnfilteredPartitionIterator augmentedData()
    {
        return null;
    }

    private SimpleBTreePartition augmentResponseInternal(PartitionUpdate update)
    {
        SimpleBTreePartition partition = data.computeIfAbsent(update.partitionKey(), key -> new SimpleBTreePartition(key, update.metadata(), UpdateTransaction.NO_OP));
        partition.update(update);
        return partition;
    }

    protected boolean canAcceptUpdate(PartitionUpdate update)
    {
        return initialIteratorExhausted || !followUpBounds.contains(update.partitionKey());
    }

    @Override
    void augmentResponse(PartitionUpdate update)
    {
        // if the input iterator reached the row limit, then we can't apply any augmenting mutations that are past
        // the last materialized key. Since we wouldn't have materialized the local data for that key, applying an
        // update would cause us to return incomplete data for it.
        if (canAcceptUpdate(update))
            augmentResponseInternal(update);
        wasAugmented = true;
    }

    protected static TrackedRead.Range makeFollowUpRead(PartitionRangeReadCommand command, AbstractBounds<PartitionPosition> followUpBounds, int toQuery, ConsistencyLevel consistencyLevel, long expiresAtNanos)
    {
        DataLimits newLimits = command.limits().forShortReadRetry(toQuery);

        DataRange newDataRange = command.dataRange().forSubRange(followUpBounds);

        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        PartitionRangeReadCommand followUpCmd = command.withUpdatedLimitsAndDataRange(newLimits, newDataRange);
        ReplicaPlan.ForRangeRead replicaPlan = ReplicaPlans.forRangeRead(keyspace,
                                                                         followUpCmd.indexQueryPlan(),
                                                                         consistencyLevel,
                                                                         followUpCmd.dataRange().keyRange(),
                                                                         1);

        TrackedRead.Range read = TrackedRead.create(followUpCmd, replicaPlan);
        logger.trace("Short read detected, starting followup read {}", read);
        read.start(expiresAtNanos);
        return read;
    }

    protected abstract CompletedRead extendRead(UnfilteredPartitionIterator iterator);

    @Override
    CompletedRead createResult(UnfilteredPartitionIterator iterator)
    {
        if (wasAugmented)
            return extendRead(iterator);
        return CompletedRead.simple(iterator, command);
    }


    static class Simple extends PartialTrackedRangeRead
    {
        public Simple(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, PartitionRangeReadCommand command, UnfilteredPartitionIterator initialData)
        {
            super(executionController, cfs, startTimeNanos, command, initialData);
        }

        @Override
        public Index.Searcher searcher()
        {
            return null;
        }

        @Override
        protected CompletedRead extendRead(UnfilteredPartitionIterator iterator)
        {
            return new ExtendingCompletedRead(command, iterator, partitionsFetched, initialIteratorExhausted, followUpBounds);
        }

        @Override
        protected UnfilteredPartitionIterator filter(UnfilteredPartitionIterator partition)
        {
            Preconditions.checkState(command().rowFilter().isEmpty());
            return partition;
        }
    }


    /**
     * Since ALLOW FILTERING reads can cover a lot of partitions without returning much data, we don't want to eagerly
     * materialize partitions onto the heap and keep them there. So this filters out non-matching partitions from the
     * freezeInitialData phase. However, if reconciliation receives a mutation that applies to a previously discarded
     * partition AND the contents of that mutation matches the row filter, we also need to retry the read against that
     * partition so we don't return incomplete data. This class handles both jobs
     */
    static class Filtered extends PartialTrackedRangeRead
    {
        private final RowFilter rowFilter;
        private final RowFilter.RowFilterTransformation filter;
        private final Set<ByteBuffer> filteredKeys = new HashSet<>();
        private final SortedMap<DecoratedKey, FollowUpReadInfo> followUpReadInfo = new TreeMap<>();

        class FilteredCompletedRead extends ExtendingCompletedRead
        {
            public FilteredCompletedRead(PartitionRangeReadCommand command, UnfilteredPartitionIterator iterator, boolean partitionsFetched, boolean initialIteratorExhausted, AbstractBounds<PartitionPosition> followUpBounds)
            {
                super(command, iterator, partitionsFetched, initialIteratorExhausted, followUpBounds);
            }

            /**
             * Even if we reached the limit during materialization, if there are keys ahead of the first materialized key
             * or interleaved with them, then we need to read them
             * @return
             */
            private boolean hasInterleavedFollowupKeys()
            {
                if (followUpReadInfo.isEmpty())
                    return false;

                if (data.isEmpty())
                    return true;

                return followUpReadInfo.firstKey().compareTo(data.lastKey()) < 0;
            }

            DecoratedKey finalKey()
            {
                return data.lastKey();
            }

            @Override
            protected boolean followUpRequired()
            {
                return hasInterleavedFollowupKeys() || super.followUpRequired();
            }

            @Override
            protected Future<TrackedDataResponse> makeFollowupRead(TrackedDataResponse initialResponse, int toQuery, ConsistencyLevel consistencyLevel, long expiresAtNanos)
            {
                List<PartialTrackedRead> followUpReads = new ArrayList<>();
                return super.makeFollowupRead(initialResponse, toQuery, consistencyLevel, expiresAtNanos);
            }
        }

        public Filtered(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, PartitionRangeReadCommand command, UnfilteredPartitionIterator initialData)
        {
            super(executionController, cfs, startTimeNanos, command, initialData);
            rowFilter = command.rowFilter();
            filter = rowFilter.filter(command().metadata(), command().nowInSec());
        }

        @Override
        public Index.Searcher searcher()
        {
            return null;
        }

        @Override
        protected CompletedRead extendRead(UnfilteredPartitionIterator iterator)
        {
            // TODO: create reads for the follow up keys and the ranges, need to propagate remaining keys to next read or whatever for additional followups
            return null;
        }

        @Override
        protected synchronized boolean canAcceptUpdate(PartitionUpdate update)
        {
            DecoratedKey key = update.partitionKey();
            if (filteredKeys.contains(key))
            {
                int matches = filter.potentialMatches(update);
                if (matches > 0)
                {
                    FollowUpReadInfo info = followUpReadInfo.computeIfAbsent(key, k -> new FollowUpReadInfo());
                    info.potentialMatches +=  matches;
                }
                logger.trace("Not applying update for previously filtered partition: {}", update.partitionKey());
                return false;
            }
            return super.canAcceptUpdate(update);
        }

        @Override
        protected UnfilteredPartitionIterator filter(UnfilteredPartitionIterator partition)
        {
            return Transformation.apply(partition, new Transformation<>()
            {
                @Override
                protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
                {
                    if (Transformation.apply(partition, filter).isEmpty())
                    {
                        DecoratedKey key = partition.partitionKey();
                        data.remove(key);
                        filteredKeys.add(key.getKey());
                        partition.close();
                        return null;
                    }
                    return partition;
                }
            });
        }
    }
}
