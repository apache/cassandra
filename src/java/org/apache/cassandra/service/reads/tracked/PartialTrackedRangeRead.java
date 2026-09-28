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

import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.range.ReplicaPlanIterator;
import org.apache.cassandra.service.reads.range.ReplicaPlanMerger;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.concurrent.Future;

public abstract class PartialTrackedRangeRead extends PartialTrackedRead
{
    private static final Logger logger = LoggerFactory.getLogger(PartialTrackedRangeRead.class);

    protected static class FollowUpReadInfo
    {
        int potentialMatches = 0;
    }

    public static class FollowupRangeCommand extends PartitionRangeReadCommand
    {
        private final AbstractBounds<PartitionPosition> remainingRange;

        public FollowupRangeCommand(PartitionRangeReadCommand command,
                                    DataLimits limits,
                                    DataRange dataRange,
                                    AbstractBounds<PartitionPosition> remainingRange)
        {
            super(command.serializedAtEpoch(),
                  command.isDigestQuery(),
                  command.digestVersion(),
                  command.potentialTxnConflicts(),
                  command.metadata(),
                  command.nowInSec(),
                  command.columnFilter(),
                  command.rowFilter(),
                  limits,
                  dataRange,
                  command.indexQueryPlan(),
                  command.isTrackingWarnings());
            this.remainingRange = remainingRange;
        }

        public AbstractBounds<PartitionPosition> remainingRange()
        {
            return remainingRange;
        }
    }

    protected final PartitionRangeReadCommand command;

    /**
     * Where a follow up read of this range should resume, set once by {@link #prepareInternal}. Kept here rather than
     * read off the state because {@link #followUpBounds()} is called from another thread, without the read's lock,
     * after the read has completed and been closed.
     * <p>
     * Null only when the read materialized no partition. A read that scanned its range to the end has
     * {@code (lastPartitionKey, right]}.
     */
    private volatile AbstractBounds<PartitionPosition> followUpBounds;

    private PartialTrackedRangeRead(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, PartitionRangeReadCommand command)
    {
        super(executionController, cfs, startTimeNanos);
        this.command = command;
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
            read = new PartialTrackedRangeRead.Filtered(executionController, cfs, startTimeNanos, command);
        }
        else
        {
            read = new PartialTrackedRangeRead.Simple(executionController, cfs, startTimeNanos, command);
        }

        // not closed if prepare throws: the controller belongs to whoever created it until this read is returned, and
        // closing it here as well would release its read ordering group twice
        read.prepare(initialData);
        return read;
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    protected static class ShortReadSupport
    {
        final boolean partitionsFetched; // whether we've seen any new partitions since iteration start or last moreContents() call
        final boolean initialIteratorExhausted;
        final AbstractBounds<PartitionPosition> followUpBounds;

        ShortReadSupport(Builder builder, boolean initialIteratorExhausted, AbstractBounds<PartitionPosition> followUpBounds)
        {
            this.partitionsFetched = builder.partitionsFetched;
            this.initialIteratorExhausted = initialIteratorExhausted;
            this.followUpBounds = followUpBounds;
        }

        protected static class Builder
        {
            final ReadCommand command;
            final DataLimits.Counter counter;
            DecoratedKey lastPartitionKey; // key of the last observed partition
            boolean partitionsFetched; // whether we've seen any new partitions since iteration start or last moreContents() call

            protected Builder(ReadCommand command)
            {
                this.command = command;
                counter = command.limits().newCounter(command.nowInSec(),
                                                      false,
                                                      command.selectsFullPartition(),
                                                      command.metadata().enforceStrictLiveness());
            }

            ShortReadSupport build()
            {
                boolean initialIteratorExhausted = command.limits().isExhausted(counter);
                AbstractBounds<PartitionPosition> followUpBounds = null;
                AbstractBounds<PartitionPosition> remaining = command instanceof FollowupRangeCommand
                                                              ? ((FollowupRangeCommand) command).remainingRange()
                                                              : command.dataRange().keyRange();
                AbstractBounds<PartitionPosition> commandRange = command.dataRange().keyRange();

                if (partitionsFetched)
                {
                    followUpBounds = remaining.inclusiveRight()
                                     ? new Range<>(lastPartitionKey, remaining.right)
                                     : new ExcludingBounds<>(lastPartitionKey, remaining.right);
                    Preconditions.checkState(!followUpBounds.contains(lastPartitionKey));
                }
                else if (initialIteratorExhausted && !commandRange.right.equals(remaining.right))
                {
                    followUpBounds = remaining.inclusiveRight()
                                     ? new Range<>(commandRange.right, remaining.right)
                                     : new ExcludingBounds<>(commandRange.right, remaining.right);
                }
                return new ShortReadSupport(this, initialIteratorExhausted, followUpBounds);
            }
        }
    }

    private abstract class Materializer extends Transformation<UnfilteredRowIterator>
    {
        final SortedMap<DecoratedKey, SimpleBTreePartition> data = new TreeMap<>();
        final ShortReadSupport.Builder shortReadSupport;

        private Materializer(ReadCommand command)
        {
            this.shortReadSupport = new ShortReadSupport.Builder(command);
        }

        abstract UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iterator);

        abstract RangePrepared createRangePrepared();

        RangePrepared materialize(UnfilteredPartitionIterator inputIterator)
        {
            try (inputIterator)
            {
                UnfilteredPartitionIterator materialized = Transformation.apply(inputIterator, this);

                UnfilteredPartitionIterator filtered = filter(materialized);

                try (UnfilteredPartitionIterator iterator = shortReadSupport.counter.applyTo(filtered))
                {
                    UnfilteredPartitionIterators.consume(iterator);
                }

                return createRangePrepared();
            }
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            SimpleBTreePartition materialized = data.computeIfAbsent(partition.partitionKey(), key -> new SimpleBTreePartition(key, partition.metadata(), UpdateTransaction.NO_OP));
            materialized.update(PartitionUpdate.fromIterator(partition, command.columnFilter()));
            shortReadSupport.lastPartitionKey = partition.partitionKey();
            shortReadSupport.partitionsFetched = true;
            return queryPartition(materialized);
        }
    }

    protected abstract class RangePrepared extends Prepared
    {
        protected final SortedMap<DecoratedKey, SimpleBTreePartition> data;
        protected final ShortReadSupport shortReadSupport;
        protected boolean wasAugmented;

        public RangePrepared(SortedMap<DecoratedKey, SimpleBTreePartition> data, ShortReadSupport shortReadSupport)
        {
            this.data = data;
            this.shortReadSupport = shortReadSupport;
        }

        protected boolean canAcceptUpdate(PartitionUpdate update)
        {
            return shortReadSupport.initialIteratorExhausted || !shortReadSupport.followUpBounds.contains(update.partitionKey());
        }

        private SimpleBTreePartition augmentResponseInternal(PartitionUpdate update)
        {
            SimpleBTreePartition partition = data.computeIfAbsent(update.partitionKey(), key -> new SimpleBTreePartition(key, update.metadata(), UpdateTransaction.NO_OP));
            partition.update(update);
            return partition;
        }

        @Override
        public void augment(PartitionUpdate update)
        {
            // if the input iterator reached the row limit, then we can't apply any augmenting mutations that are past
            // the last materialized key. Since we wouldn't have materialized the local data for that key, applying an
            // update would cause us to return incomplete data for it.
            if (canAcceptUpdate(update))
            {
                logger.trace("Augmented partition {} for read {}", update.partitionKey(), PartialTrackedRangeRead.this);
                augmentResponseInternal(update);
            }
            else
            {
                logger.trace("Ignoring unacceptable update from key {} on read {}", update.partitionKey(), PartialTrackedRangeRead.this);
            }
            wasAugmented = true;
        }
    }

    protected abstract class RangeCompleted extends AbstractCompleted
    {
        protected final SortedMap<DecoratedKey, SimpleBTreePartition> data;
        protected final ShortReadSupport shortReadSupport;
        protected final boolean wasAugmented;

        public RangeCompleted(SortedMap<DecoratedKey, SimpleBTreePartition> data, ShortReadSupport shortReadSupport, boolean wasAugmented)
        {
            this.data = data;
            this.shortReadSupport = shortReadSupport;
            this.wasAugmented = wasAugmented;
        }

        @Override
        protected UnfilteredPartitionIterator iterator()
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

        protected abstract CompletedRead extendRead(UnfilteredPartitionIterator iterator);

        @Override
        protected CompletedRead createResult(UnfilteredPartitionIterator iterator)
        {
            if (wasAugmented)
                return extendRead(iterator);
            return CompletedRead.simple(iterator, command, command.nowInSec());
        }
    }

    abstract Materializer createMaterializer();

    @Override
    protected Prepared prepareInternal(UnfilteredPartitionIterator initialData)
    {
        // memtable contents are frozen at read completion time, when the iterator is evaluated, not at the beginning
        // of the read, when references to memtables and sstables are collected. Because of this, replica coordinated
        // reads can cause read monotonicity to be broken by returning data that hasn't been replicated to at least
        // CL other nodes via reconciliation. To prevent this, the contents of the initial iterator are materialized
        // onto heap at partition granularity until the limits of the read are reached.

        Materializer materializer = createMaterializer();
        RangePrepared prepared = materializer.materialize(initialData);
        followUpBounds = prepared.shortReadSupport.followUpBounds;
        return prepared;
    }

    UnfilteredRowIterator queryPartition(AbstractBTreePartition partition)
    {
        return partition.unfilteredIterator(command.columnFilter(),
                                            command.requestedSlices(),
                                            command.clusteringIndexFilter(partition.partitionKey()).isReversed());
    }

    public AbstractBounds<PartitionPosition> followUpBounds()
    {
        return followUpBounds;
    }

    protected static TrackedRead.Range makeFollowUpRead(PartitionRangeReadCommand command, AbstractBounds<PartitionPosition> followUpBounds, int toQuery, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        DataLimits newLimits = command.limits().forShortReadRetry(toQuery);

        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);

        // RangeCommandIterator opportunistically merges commands for adjacent ranges that have different replica
        // sets when their replica plans select the same endpoints to contact.
        // For instance, range A belongs to node [1,2,3], and range B belongs to nodes [2,3,4]. If both reads select
        // [2,3], they'll be merged into a single command.
        //
        // This creates a problem for followup reads since the followup range can then straddle replica sets.
        // ReplicaPlans.forRangeRead expects ranges to already be split across replica set boundaries and selects replicas
        // based on range.right - if any of the replicas selected for the followup read don't happen to replicate range A,
        // the read can't complete, and may return incorrect information if downstream validation is weak.
        //
        // To prevent silently dropping data from range A, we have ReplicaPlanIterator and ReplicaPlanMerger re-split
        // and (attempt to) re-merge the followup bounds, creating the followup read command from the first bounds it
        // emits. We also communicate the original followup bounds to the next PartialTrackedRangeRead instance via
        // FollowupRangeCommand so it will complete the rest of followUpBounds if the replicaPlan emitted below doesn't
        // fully cover the original followup bounds
        ReplicaPlanIterator replicaPlans = new ReplicaPlanIterator(followUpBounds,
                                                                   command.indexQueryPlan(),
                                                                   keyspace,
                                                                   command.metadata().id,
                                                                   consistencyLevel);
        ReplicaPlanMerger mergedReplicaPlans = new ReplicaPlanMerger(replicaPlans,
                                                                     keyspace,
                                                                     command.metadata().id,
                                                                     consistencyLevel);
        ReplicaPlan.ForRangeRead replicaPlan = mergedReplicaPlans.next();
        DataRange newDataRange = command.dataRange().forSubRange(replicaPlan.range());
        PartitionRangeReadCommand followUpCmd = new FollowupRangeCommand(command, newLimits, newDataRange, followUpBounds);

        TrackedRead.Range read = TrackedRead.Range.create(followUpCmd, replicaPlan, requestTime);
        logger.trace("Short read detected, starting followup read {}", read);
        return read;
    }

    static class Simple extends PartialTrackedRangeRead
    {
        private class SimplePrepared extends RangePrepared
        {
            public SimplePrepared(SortedMap<DecoratedKey, SimpleBTreePartition> data, ShortReadSupport shortReadSupport)
            {
                super(data, shortReadSupport);
            }

            @Override
            Completed complete()
            {
                return new SimpleCompleted(data, shortReadSupport, wasAugmented);
            }
        }

        protected class SimpleCompleted extends RangeCompleted
        {
            public SimpleCompleted(SortedMap<DecoratedKey, SimpleBTreePartition> data, ShortReadSupport shortReadSupport, boolean wasAugmented)
            {
                super(data, shortReadSupport, wasAugmented);
            }

            @Override
            protected CompletedRead extendRead(UnfilteredPartitionIterator iterator)
            {
                return new ExtendingCompletedRead.RangeRead(command, iterator, shortReadSupport.partitionsFetched, shortReadSupport.initialIteratorExhausted, shortReadSupport.followUpBounds);
            }
        }

        public Simple(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, PartitionRangeReadCommand command)
        {
            super(executionController, cfs, startTimeNanos, command);
        }

        @Override
        Materializer createMaterializer()
        {
            return new Materializer(command)
            {
                @Override
                UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iterator)
                {
                    Preconditions.checkState(command().rowFilter().isEmpty());
                    return iterator;
                }

                @Override
                RangePrepared createRangePrepared()
                {
                    return new SimplePrepared(data, shortReadSupport.build());
                }
            };
        }

        @Override
        public Index.Searcher searcher()
        {
            return null;
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

        protected class FilteredPrepared extends RangePrepared
        {
            private final Set<DecoratedKey> filteredKeys;
            private final SortedMap<DecoratedKey, FollowUpReadInfo> followUpReadInfo = new TreeMap<>();
            private final RowFilter.RowFilterTransformation filter;
            public FilteredPrepared(SortedMap<DecoratedKey, SimpleBTreePartition> data, ShortReadSupport shortReadSupport, Set<DecoratedKey> filteredKeys, RowFilter.RowFilterTransformation filter)
            {
                super(data, shortReadSupport);
                this.filteredKeys = filteredKeys;
                this.filter = filter;
            }

            @Override
            protected boolean canAcceptUpdate(PartitionUpdate update)
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
            Completed complete()
            {
                return new FilteredCompleted(data, shortReadSupport, wasAugmented, followUpReadInfo);
            }
        }

        protected class FilteredMaterializer extends Materializer
        {
            private final Set<DecoratedKey> filteredKeys = new HashSet<>();
            private final RowFilter.RowFilterTransformation filter;
            public FilteredMaterializer(ReadCommand command)
            {
                super(command);
                filter = command.rowFilter().filter(command().metadata(), command().nowInSec());
            }

            private void markFiltered(DecoratedKey key)
            {
                data.remove(key);
                filteredKeys.add(key);
            }

            @Override
            UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iterator)
            {
                return Transformation.apply(iterator, new Transformation<>()
                {
                    @Override
                    protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
                    {
                        DecoratedKey key = partition.partitionKey();

                        UnfilteredPartitionIterator filtered = Transformation.apply(new SingletonUnfilteredPartitionIterator(partition), filter);
                        if (!filtered.hasNext())
                        {
                            markFiltered(key);
                            return null;
                        }

                        return Transformation.apply(filtered.next(), new Transformation<UnfilteredRowIterator>()
                        {
                            int rows = 0;

                            @Override
                            protected void onPartitionClose()
                            {
                                if (rows == 0)
                                    markFiltered(key);
                                super.onPartitionClose();
                            }

                            @Override
                            protected Row applyToRow(Row row)
                            {
                                if (row.hasLiveData(command.nowInSec(), command.metadata().enforceStrictLiveness()))
                                    rows++;
                                return super.applyToRow(row);
                            }

                            @Override
                            protected Row applyToStatic(Row row)
                            {
                                // we only count static rows as matches if there are no clustering or regular column
                                // matches because `WHERE static_col=7 AND regular_col>2 ALLOW FILTERING` requires normal
                                // rows to exist, and the counter applyToRow will increment rows only if the static clause
                                // is true which would have been evaluated earlier in `applyToPartition`
                                if (!command.rowFilter().hasExpressionOnClusteringOrRegularColumns()
                                    && row.hasLiveData(command.nowInSec(), command.metadata().enforceStrictLiveness()))
                                    rows++;
                                return super.applyToStatic(row);
                            }
                        });
                    }
                });
            }

            @Override
            RangePrepared createRangePrepared()
            {
                return new FilteredPrepared(data, shortReadSupport.build(), filteredKeys, filter);
            }
        }

        static class FilteredCompletedRead extends ExtendingCompletedRead.RangeRead
        {
            private final DecoratedKey lastMatchingKey;
            private final SortedMap<DecoratedKey, FollowUpReadInfo> followUpReadInfo;
            public FilteredCompletedRead(PartitionRangeReadCommand command, UnfilteredPartitionIterator iterator, ShortReadSupport shortReadSupport, DecoratedKey lastMatchingKey, SortedMap<DecoratedKey, FollowUpReadInfo> followUpReadInfo)
            {
                super(command, iterator, shortReadSupport.partitionsFetched, shortReadSupport.initialIteratorExhausted, shortReadSupport.followUpBounds);
                this.lastMatchingKey = lastMatchingKey;
                this.followUpReadInfo = followUpReadInfo;
            }

            /**
             * Even if we reached the limit during materialization, if there are keys ahead of the first materialized key
             * or interleaved with them, then we need to read them
             */
            private boolean hasInterleavedFollowupKeys()
            {
                if (followUpReadInfo.isEmpty())
                    return false;

                if (lastMatchingKey == null)  // no data, so nothing to displace; same answer as hasRoomForFollowUpKeys
                    return true;

                return followUpReadInfo.firstKey().compareTo(lastMatchingKey) < 0;
            }

            /**
             * Whether there are keys to read again and the answer is still short of its limit.
             * <p>
             * A key in {@code followUpReadInfo} is a partition the row filter dropped during the scan that a write this
             * replica was missing could make match; {@link FilteredPrepared#canAcceptUpdate} refuses those writes, so
             * the partition is read again from storage. Short read protection would not read these keys: it reads only
             * forward from {@link #followUpBounds}, and every one of them sorts at or before the last key the scan
             * visited.
             */
            private boolean hasRoomForFollowUpKeys()
            {
                return !followUpReadInfo.isEmpty() && !mergedResultCounter.isDone();
            }

            @Override
            protected boolean followUpRequired()
            {
                return hasInterleavedFollowupKeys() || hasRoomForFollowUpKeys() || super.followUpRequired();
            }

            @Override
            protected Future<TrackedDataResponse> makeFollowupRead(TrackedDataResponse initialResponse, int toQuery, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
            {
                if (followUpReadInfo.isEmpty())
                    return super.makeFollowupRead(initialResponse, toQuery, consistencyLevel, requestTime);

                FilteredFollowupRead followupRead = new FilteredFollowupRead(initialResponse,
                                                                             toQuery,
                                                                             consistencyLevel,
                                                                             requestTime,
                                                                             followUpReadInfo,
                                                                             command,
                                                                             followUpBounds,
                                                                             lastMatchingKey);

                followupRead.start();
                return followupRead;
            }

        }

        private class FilteredCompleted extends RangeCompleted
        {
            private final SortedMap<DecoratedKey, FollowUpReadInfo> followUpReadInfo;
            public FilteredCompleted(SortedMap<DecoratedKey, SimpleBTreePartition> data, ShortReadSupport shortReadSupport, boolean wasAugmented, SortedMap<DecoratedKey, FollowUpReadInfo> followUpReadInfo)
            {
                super(data, shortReadSupport, wasAugmented);
                this.followUpReadInfo = followUpReadInfo;
            }

            @Override
            protected CompletedRead extendRead(UnfilteredPartitionIterator iterator)
            {
                // data also holds partitions the missing writes created or changed, and the row filter applied on
                // completion can still drop its last key; that can only make hasInterleavedFollowupKeys read a key it
                // did not need to, never skip one
                return new FilteredCompletedRead(command, iterator, shortReadSupport, data.isEmpty() ? null : data.lastKey(), followUpReadInfo);
            }
        }

        public Filtered(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, PartitionRangeReadCommand command)
        {
            super(executionController, cfs, startTimeNanos, command);
        }

        @Override
        Materializer createMaterializer()
        {
            return new FilteredMaterializer(command);
        }

        @Override
        public Index.Searcher searcher()
        {
            return null;
        }
    }
}
