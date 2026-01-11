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
package org.apache.cassandra.service.accord.journal;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.MaxDecidedRX;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.UncheckedInterruptedException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.accord.OrderedRouteSerializer;
import org.apache.cassandra.index.accord.RouteJournalIndex;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.journal.RangeSearcher.NoopJournalRangeSearcher;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MergeIterator;

import static org.apache.cassandra.service.accord.AccordKeyspace.JournalColumns.getJournalKey;

public class RangeSearchManager implements RangeSearcher.Supplier
{
    private static final Logger logger = LoggerFactory.getLogger(RangeSearchManager.class);

    final ColumnFamilyStore cfs;

    /**
     * Access to this field should only ever be handled by {@link #safeNotify(Consumer)}.  There is an assumption that
     * an error in the index should not cause the journal to crash, so {@link #safeNotify(Consumer)} exists to make sure
     * this property holds true.
     */
    @Nullable
    private final SegmentRangeSearcher<Object> segmentSearcher;

    private RangeSearchManager(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.segmentSearcher = new SegmentRangeSearcher<>();
    }

    static @Nullable RangeSearchManager ifEnabled(ColumnFamilyStore cfs)
    {
        return cfs.indexManager.getIndexByName(AccordKeyspace.JOURNAL_INDEX_NAME) != null ?
               new RangeSearchManager(cfs) : null;
    }

    org.apache.cassandra.journal.SegmentCompactor<JournalKey, Object> compactor(ColumnFamilyStore cfs, Version userVersion)
    {
        return new SegmentCompactor<>(userVersion, cfs) {
            @Nullable
            @Override
            public Collection<StaticSegment<JournalKey, Object>> compact(Collection<StaticSegment<JournalKey, Object>> staticSegments)
            {
                Collection<StaticSegment<JournalKey, Object>> result = super.compact(staticSegments);
                safeNotify(index -> index.remove(staticSegments));
                return result;
            }
        };
    }

    Runnable maybeIndex(JournalKey key, RecordPointer pointer, CommandChangeWriter change)
    {
        if (shouldIndex(key)
            && change.hasParticipants()
            && change.after.route() != null)
        {
            return () -> safeNotify(index -> index.update(pointer.segment, key.commandStoreId, key.id, change.after.route()));
        }
        return null;
    }

    boolean shouldIndex(JournalKey key)
    {
        return RouteJournalIndex.allowed(key);
    }

    void safeNotify(Consumer<SegmentRangeSearcher<Object>> fn)
    {
        if (segmentSearcher == null)
            return;
        try
        {
            fn.accept(segmentSearcher);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Failure updating index", t);
        }
    }

    @Override
    public RangeSearcher rangeSearcher()
    {
        if (segmentSearcher == null)
            return NoopJournalRangeSearcher.instance;
        return new JournalTableRangeSearcher();
    }

    public void start()
    {
        if (segmentSearcher == null) return;
        Index tableIndex = cfs.indexManager.getIndexByName(AccordKeyspace.JOURNAL_INDEX_NAME);
        RetryStrategy retry = DatabaseDescriptor.getAccord().retry_journal_index_ready.retry();
        for (int i = 0; !cfs.indexManager.isIndexQueryable(tableIndex); i++)
        {
            logger.debug("Journal index {} is not ready wait... waiting", AccordKeyspace.JOURNAL_INDEX_NAME);
            maybeWait(retry, i);
        }
    }

    /**
     * This method is here to make it easier for org.apache.cassandra.distributed.test.accord.journal.JournalAccessRouteIndexOnStartupRaceTest
     * to check when we need to do waiting
     */
    @VisibleForTesting
    private static void maybeWait(RetryStrategy retry, int i)
    {
        long waitTime = retry.computeWait(i, TimeUnit.MICROSECONDS);
        if (waitTime == -1)
            throw new IllegalStateException("Gave up waiting on journal index to be ready");
        try
        {
            TimeUnit.MICROSECONDS.sleep(waitTime);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    /**
     * When using {@link PartitionRangeReadCommand} we need to work with {@link RowFilter} which works with columns.
     * But the index doesn't care about table based queries and needs to be queried using the fields in the index, to
     * support that this enum exists.  This enum represents the fields present in the index and can be used to apply
     * filters to the index.
     */
    public enum SyntheticColumn
    {
        participants("participants", BytesType.instance),
        store_id("store_id", Int32Type.instance),
        txn_id("txn_id", BytesType.instance);

        public final ColumnMetadata metadata;

        SyntheticColumn(String name, AbstractType<?> type)
        {
            this.metadata = new ColumnMetadata("journal", "routes", new ColumnIdentifier(name, false), type, ColumnMetadata.NO_UNIQUE_ID, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null);
        }
    }

    private class JournalTableRangeSearcher implements RangeSearcher
    {
        private final Index tableIndex;

        private JournalTableRangeSearcher()
        {
            this.tableIndex = cfs.indexManager.getIndexByName("record");
            if (!cfs.indexManager.isIndexQueryable(tableIndex))
                throw new AssertionError("Journal record index is not queryable");
        }

        @Override
        public Result search(int commandStoreId, TokenRange range, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX)
        {
            CloseableIterator<TxnId> inMemory = segmentSearcher.search(commandStoreId, range, minTxnId, maxTxnId, decidedRX).results();
            CloseableIterator<TxnId> table = tableSearch(commandStoreId, range.start(), range.end(), minTxnId, maxTxnId, decidedRX);
            return new DefaultResult(minTxnId, maxTxnId, decidedRX, MergeIterator.get(Arrays.asList(inMemory, table)));
        }

        @Override
        public Result search(int commandStoreId, TokenKey key, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX)
        {
            CloseableIterator<TxnId> inMemory = segmentSearcher.search(commandStoreId, key, minTxnId, maxTxnId, decidedRX).results();
            CloseableIterator<TxnId> table = tableSearch(commandStoreId, key, minTxnId, maxTxnId);
            return new DefaultResult(minTxnId, maxTxnId, decidedRX, MergeIterator.get(Arrays.asList(inMemory, table)));
        }

        private CloseableIterator<TxnId> tableSearch(int store, TokenKey start, TokenKey end, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX)
        {
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(RangeSearchManager.SyntheticColumn.participants.metadata, Operator.GT, OrderedRouteSerializer.serialize(start));
            rowFilter.add(RangeSearchManager.SyntheticColumn.participants.metadata, Operator.LTE, OrderedRouteSerializer.serialize(end));
            rowFilter.add(RangeSearchManager.SyntheticColumn.store_id.metadata, Operator.EQ, Int32Type.instance.decompose(store));
            rowFilter.add(RangeSearchManager.SyntheticColumn.txn_id.metadata, Operator.GTE, CommandSerializers.txnId.serialize(minTxnId));
            rowFilter.add(RangeSearchManager.SyntheticColumn.txn_id.metadata, Operator.LTE, CommandSerializers.timestamp.serialize(maxTxnId));
            return process(store, rowFilter);
        }

        private CloseableIterator<TxnId> tableSearch(int store, TokenKey key, TxnId minTxnId, Timestamp maxTxnId)
        {
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(RangeSearchManager.SyntheticColumn.participants.metadata, Operator.GTE, OrderedRouteSerializer.serialize(key));
            rowFilter.add(RangeSearchManager.SyntheticColumn.participants.metadata, Operator.LTE, OrderedRouteSerializer.serialize(key));
            rowFilter.add(RangeSearchManager.SyntheticColumn.store_id.metadata, Operator.EQ, Int32Type.instance.decompose(store));
            rowFilter.add(RangeSearchManager.SyntheticColumn.txn_id.metadata, Operator.GTE, CommandSerializers.txnId.serialize(minTxnId));
            rowFilter.add(RangeSearchManager.SyntheticColumn.txn_id.metadata, Operator.LTE, CommandSerializers.timestamp.serialize(maxTxnId));
            return process(store, rowFilter);
        }

        private CloseableIterator<TxnId> process(int storeId, RowFilter rowFilter)
        {
            PartitionRangeReadCommand cmd = PartitionRangeReadCommand.create(cfs.metadata(),
                                                                             FBUtilities.nowInSeconds(),
                                                                             ColumnFilter.selectionBuilder()
                                                                                         .add(RangeSearchManager.SyntheticColumn.store_id.metadata)
                                                                                         .add(RangeSearchManager.SyntheticColumn.txn_id.metadata)
                                                                                         .build(),
                                                                             rowFilter,
                                                                             DataLimits.NONE,
                                                                             DataRange.allData(cfs.getPartitioner()));
            Index.Searcher s = tableIndex.searcherFor(cmd);
            try (ReadExecutionController controller = cmd.executionController())
            {
                UnfilteredPartitionIterator partitionIterator = s.search(controller);
                return new CloseableIterator<>()
                {

                    @Override
                    public void close()
                    {
                        partitionIterator.close();
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return partitionIterator.hasNext();
                    }

                    @Override
                    public TxnId next()
                    {
                        UnfilteredRowIterator next = partitionIterator.next();
                        JournalKey partitionKeyComponents = getJournalKey(next.partitionKey());
                        Invariants.require(partitionKeyComponents.commandStoreId == storeId,
                                              () -> String.format("table index returned a command store other than the exepcted one; expected %d != %d", storeId, partitionKeyComponents.commandStoreId));
                        return partitionKeyComponents.id;
                    }
                };
            }
        }
    }
}