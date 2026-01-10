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
package org.apache.cassandra.service.accord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;

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
import org.apache.cassandra.db.ColumnFamilyStore.RefViewFragment;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.StorageHook;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.accord.OrderedRouteSerializer;
import org.apache.cassandra.index.accord.RouteJournalIndex;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.journal.RecordConsumer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.accord.AccordKeyspace.JournalColumns;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.io.sstable.SSTableReadsListener.NOOP_LISTENER;
import static org.apache.cassandra.service.accord.AccordKeyspace.JournalColumns.getJournalKey;
import static org.apache.cassandra.service.accord.JournalKey.SUPPORT;

public class AccordJournalTable<V> implements JournalRangeSearcher.Supplier
{
    private static final Logger logger = LoggerFactory.getLogger(AccordJournalTable.class);

    final ColumnFamilyStore cfs;

    /**
     * Access to this field should only ever be handled by {@link #safeNotify(Consumer)}.  There is an assumption that
     * an error in the index should not cause the journal to crash, so {@link #safeNotify(Consumer)} exists to make sure
     * this property holds true.
     */
    @Nullable
    private final JournalSegmentRangeSearcher<Object> index;

    public AccordJournalTable(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;

        this.index = cfs.indexManager.getIndexByName(AccordKeyspace.JOURNAL_INDEX_NAME) != null
                     ? new JournalSegmentRangeSearcher<>()
                     : null;
    }

    boolean shouldIndex(JournalKey key)
    {
        if (index == null) return false;
        return RouteJournalIndex.allowed(key);
    }

    void safeNotify(Consumer<JournalSegmentRangeSearcher<Object>> fn)
    {
        if (index == null)
            return;
        try
        {
            fn.accept(index);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Failure updating index", t);
        }
    }

    public void forceCompaction()
    {
        cfs.forceMajorCompaction();
    }

    @Override
    public JournalRangeSearcher rangeSearcher()
    {
        if (index == null)
            return JournalRangeSearcher.NoopJournalRangeSearcher.instance;
        return new JournalTableRangeSearcher();
    }

    public void start()
    {
        if (index == null) return;
        Index tableIndex = cfs.indexManager.getIndexByName(AccordKeyspace.JOURNAL_INDEX_NAME);
        RetryStrategy retry = DatabaseDescriptor.getAccord().retry_journal_index_ready.retry();
        for (int i = 0; !cfs.indexManager.isIndexQueryable(tableIndex); i++)
        {
            logger.debug("Journal index {} is not ready wait... waiting", AccordKeyspace.JOURNAL_INDEX_NAME);
            maybeWait(retry, i);
        }
    }

    public long maxDescriptor()
    {
        return cfs.getTracker().getView().liveSSTables()
                  .stream()
                  .filter(sst -> sst.getSSTableMetadata().totalRows > 0)
                  .map(sst -> LongType.instance.compose(sst.getSSTableMetadata().coveredClustering.end().bufferAt(0)))
                  .max(Long::compare).orElse(0L);
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

    private class JournalTableRangeSearcher implements JournalRangeSearcher
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
            CloseableIterator<TxnId> inMemory = index.search(commandStoreId, range, minTxnId, maxTxnId, decidedRX).results();
            CloseableIterator<TxnId> table = tableSearch(commandStoreId, range.start(), range.end(), minTxnId, maxTxnId, decidedRX);
            return new DefaultResult(minTxnId, maxTxnId, decidedRX, MergeIterator.get(Arrays.asList(inMemory, table)));
        }

        @Override
        public Result search(int commandStoreId, TokenKey key, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX)
        {
            CloseableIterator<TxnId> inMemory = index.search(commandStoreId, key, minTxnId, maxTxnId, decidedRX).results();
            CloseableIterator<TxnId> table = tableSearch(commandStoreId, key, minTxnId, maxTxnId);
            return new DefaultResult(minTxnId, maxTxnId, decidedRX, MergeIterator.get(Arrays.asList(inMemory, table)));
        }

        private CloseableIterator<TxnId> tableSearch(int store, TokenKey start, TokenKey end, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX)
        {
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(AccordJournalTable.SyntheticColumn.participants.metadata, Operator.GT, OrderedRouteSerializer.serialize(start));
            rowFilter.add(AccordJournalTable.SyntheticColumn.participants.metadata, Operator.LTE, OrderedRouteSerializer.serialize(end));
            rowFilter.add(AccordJournalTable.SyntheticColumn.store_id.metadata, Operator.EQ, Int32Type.instance.decompose(store));
            rowFilter.add(AccordJournalTable.SyntheticColumn.txn_id.metadata, Operator.GTE, CommandSerializers.txnId.serialize(minTxnId));
            rowFilter.add(AccordJournalTable.SyntheticColumn.txn_id.metadata, Operator.LTE, CommandSerializers.timestamp.serialize(maxTxnId));
            return process(store, rowFilter);
        }

        private CloseableIterator<TxnId> tableSearch(int store, TokenKey key, TxnId minTxnId, Timestamp maxTxnId)
        {
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(AccordJournalTable.SyntheticColumn.participants.metadata, Operator.GTE, OrderedRouteSerializer.serialize(key));
            rowFilter.add(AccordJournalTable.SyntheticColumn.participants.metadata, Operator.LTE, OrderedRouteSerializer.serialize(key));
            rowFilter.add(AccordJournalTable.SyntheticColumn.store_id.metadata, Operator.EQ, Int32Type.instance.decompose(store));
            rowFilter.add(AccordJournalTable.SyntheticColumn.txn_id.metadata, Operator.GTE, CommandSerializers.txnId.serialize(minTxnId));
            rowFilter.add(AccordJournalTable.SyntheticColumn.txn_id.metadata, Operator.LTE, CommandSerializers.timestamp.serialize(maxTxnId));
            return process(store, rowFilter);
        }

        private CloseableIterator<TxnId> process(int storeId, RowFilter rowFilter)
        {
            PartitionRangeReadCommand cmd = PartitionRangeReadCommand.create(cfs.metadata(),
                                                                             FBUtilities.nowInSeconds(),
                                                                             ColumnFilter.selectionBuilder()
                                                                                         .add(AccordJournalTable.SyntheticColumn.store_id.metadata)
                                                                                         .add(AccordJournalTable.SyntheticColumn.txn_id.metadata)
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

    static class TableKeyIterator implements Closeable, RecordConsumer<JournalKey>
    {
        final JournalKey key;
        final List<UnfilteredRowIterator> unmerged;
        final UnfilteredRowIterator merged;

        long segment;
        int offset;
        ByteBuffer value;
        int userVersion;

        TableKeyIterator(JournalKey key, List<UnfilteredRowIterator> unmerged, UnfilteredRowIterator merged)
        {
            this.key = key;
            this.unmerged = unmerged;
            this.merged = merged;
        }

        @Override
        public void accept(long segment, int offset, JournalKey key, ByteBuffer buffer, int userVersion)
        {
            this.segment = segment;
            this.offset = offset;
            this.value = buffer;
            this.userVersion = userVersion;
        }

        boolean advance()
        {
            if (merged == null || !merged.hasNext())
                return false;

            try
            {
                Row row = (Row) merged.next();
                segment = LongType.instance.compose(ByteBuffer.wrap((byte[]) row.clustering().get(0)));
                offset = Int32Type.instance.compose(ByteBuffer.wrap((byte[]) row.clustering().get(1)));
                value = row.getCell(JournalColumns.record).buffer();
                userVersion = Int32Type.instance.compose(row.getCell(JournalColumns.user_version).buffer());
                return true;
            }
            catch (Throwable t)
            {
                throw new FSReadError("Failed to read from " + unmerged, t);
            }
        }

        @Override
        public void close()
        {
            if (merged != null)
                merged.close();
        }
    }

    TableKeyIterator readAllFromTable(JournalKey key, OpOrder.Group readOrder)
    {
        DecoratedKey pk = JournalColumns.decorate(key);
        List<UnfilteredRowIterator> iters = new ArrayList<>(3);
        try
        {
            ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, pk));
            for (SSTableReader sstable : view.sstables)
            {
                if (!sstable.mayContainAssumingKeyIsInRange(pk))
                    continue;

                UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs, sstable, pk, Slices.ALL, ColumnFilter.all(cfs.metadata()), false, NOOP_LISTENER);
                if (iter.getClass() != EmptyIterators.EmptyUnfilteredRowIterator.class)
                    iters.add(iter);
            }

            return new TableKeyIterator(key, iters, iters.isEmpty() ? null : UnfilteredRowIterators.merge(iters));
        }
        catch (Throwable t)
        {
            for (UnfilteredRowIterator iter : iters)
            {
                try { iter.close(); }
                catch (Throwable t2) { t.addSuppressed(t2); }
            }
            throw t;
        }
    }

    TableIterator keyIterator(JournalKey min, JournalKey max, long minSegment)
    {
        return new TableIterator(cfs, min, max, minSegment);
    }

    static class TableIterator extends AbstractIterator<JournalKey> implements CloseableIterator<JournalKey>
    {
        private final UnfilteredPartitionIterator mergeIterator;
        private final RefViewFragment view;

        private TableIterator(ColumnFamilyStore table, JournalKey min, JournalKey max, long minSegment)
        {
            Invariants.require((min != null && max != null) || min == max);
            view = table.selectAndReference(View.select(SSTableSet.LIVE, r -> (max == null || SUPPORT.compare(getJournalKey(r.getFirst()), max) <= 0)
                                                                         && (min == null || SUPPORT.compare(getJournalKey(r.getLast()), min) >= 0)
                                                                         && (r.getSSTableMetadata().coveredClustering.end().isArtificial() || LongType.instance.compose(r.getSSTableMetadata().coveredClustering.end().bufferAt(0)) >= minSegment)
            ));
            List<ISSTableScanner> scanners = new ArrayList<>();
            for (SSTableReader sstable : view.sstables)
            {

                if (min == null) scanners.add(sstable.getScanner());
                else scanners.add(sstable.getScanner(new Bounds(JournalColumns.decorate(min), JournalColumns.decorate(max))));
            }

            mergeIterator = view.sstables.isEmpty()
                            ? EmptyIterators.unfilteredPartition(table.metadata())
                            : UnfilteredPartitionIterators.merge(scanners, UnfilteredPartitionIterators.MergeListener.NOOP);
        }

        @CheckForNull
        protected JournalKey computeNext()
        {
            JournalKey ret = null;
            if (mergeIterator.hasNext())
            {
                try (UnfilteredRowIterator partition = mergeIterator.next())
                {
                    ret = getJournalKey(partition.partitionKey());
                    while (partition.hasNext())
                        partition.next();
                }
            }

            if (ret != null)
                return ret;
            else
                return endOfData();
        }

        @Override
        public void close()
        {
            mergeIterator.close();
            view.close();
        }
    }
}