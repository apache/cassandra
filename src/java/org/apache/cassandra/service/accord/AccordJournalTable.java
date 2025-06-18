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

import java.io.IOException;
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

import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.UncheckedInterruptedException;
import org.agrona.collections.LongHashSet;
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
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.accord.OrderedRouteSerializer;
import org.apache.cassandra.index.accord.RouteJournalIndex;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.journal.EntrySerializer.EntryHolder;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.RecordConsumer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MergeIterator;

import static org.apache.cassandra.io.sstable.SSTableReadsListener.NOOP_LISTENER;

public class AccordJournalTable<K extends JournalKey, V> implements RangeSearcher.Supplier
{
    private static final Logger logger = LoggerFactory.getLogger(AccordJournalTable.class);

    private final Journal<K, V> journal;
    private final ColumnFamilyStore cfs;

    private final ColumnMetadata recordColumn;
    private final ColumnMetadata versionColumn;

    private final KeySupport<K> keySupport;
    /**
     * Access to this field should only ever be handled by {@link #safeNotify(Consumer)}.  There is an assumption that
     * an error in the index should not cause the journal to crash, so {@link #safeNotify(Consumer)} exists to make sure
     * this property holds true.
     */
    @Nullable
    private final RouteInMemoryIndex<Object> index;
    private final Version accordJournalVersion;

    public AccordJournalTable(Journal<K, V> journal, KeySupport<K> keySupport, ColumnFamilyStore cfs, Version accordJournalVersion)
    {
        this.journal = journal;
        this.cfs = cfs;
        this.recordColumn = cfs.metadata().getColumn(ColumnIdentifier.getInterned("record", false));
        this.versionColumn = cfs.metadata().getColumn(ColumnIdentifier.getInterned("user_version", false));
        this.keySupport = keySupport;
        this.accordJournalVersion = accordJournalVersion;

        this.index = cfs.indexManager.getIndexByName(AccordKeyspace.JOURNAL_INDEX_NAME) != null
                     ? new RouteInMemoryIndex<>()
                     : null;
    }

    boolean shouldIndex(JournalKey key)
    {
        if (index == null) return false;
        return RouteJournalIndex.allowed(key);
    }

    void safeNotify(Consumer<RouteInMemoryIndex<Object>> fn)
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
    public RangeSearcher rangeSearcher()
    {
        if (index == null)
            return RangeSearcher.NoopRangeSearcher.instance;
        return new TableRangeSearcher();
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

    public interface Reader
    {
        void read(DataInputPlus input, Version userVersion) throws IOException;
    }

    private static class RecordConsumerAdapter<K> implements RecordConsumer<K>
    {
        protected final Reader reader;

        RecordConsumerAdapter(Reader reader)
        {
            this.reader = reader;
        }

        private long prevSegment = Long.MAX_VALUE;
        private long prevPosition = Long.MAX_VALUE;

        @Override
        public void accept(long segment, int position, K key, ByteBuffer buffer, int userVersion)
        {
            Invariants.require(segment <= prevSegment,
                               "Records should always be iterated over in a reverse order, but segment %d was seen after %d while reading %s", segment, prevSegment, key);
            Invariants.require(segment != prevSegment || position < prevPosition,
                               "Records should always be iterated over in a reverse order, but position %d was seen after %d for segment %d while reading %s", position, prevPosition, segment, key);
            readBuffer(buffer, reader, Version.fromVersion(userVersion));
            prevSegment = segment;
            prevPosition = position;
        }
    }

    // TODO (expected): this can be removed entirely when we "flush" segments directly to sstables (but we perhaps need to be careful about the active segment)
    private class TableRecordConsumer implements RecordConsumer<K>
    {
        final LongHashSet visited;
        final RecordConsumer<K> delegate;

        TableRecordConsumer(LongHashSet visited, RecordConsumer<K> delegate)
        {
            this.visited = visited;
            this.delegate = delegate;
        }

        boolean visited(long segment)
        {
            return visited != null && visited.contains(segment);
        }

        @Override
        public void accept(long segment, int position, K key, ByteBuffer buffer, int userVersion)
        {
            if (!visited(segment))
                delegate.accept(segment, position, key, buffer, userVersion);
        }
    }

    private class JournalAndTableRecordConsumer implements RecordConsumer<K>
    {
        private final K key;
        private final RecordConsumer<K> delegate;
        private LongHashSet visited;

        void visit(long segment)
        {
            if (visited == null)
                visited = new LongHashSet();
            visited.add(segment);
        }

        JournalAndTableRecordConsumer(K key, RecordConsumer<K> reader)
        {
            this.key = key;
            this.delegate = reader;
        }

        void readTable()
        {
            readAllFromTable(key, new TableRecordConsumer(visited, delegate));
        }

        @Override
        public void accept(long segment, int position, K key, ByteBuffer buffer, int userVersion)
        {
            visit(segment);
            delegate.accept(segment, position, key, buffer, userVersion);
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
        txn_id("txn_id", AccordKeyspace.TIMESTAMP_TYPE);

        public final ColumnMetadata metadata;

        SyntheticColumn(String name, AbstractType<?> type)
        {
            this.metadata = new ColumnMetadata("journal", "routes", new ColumnIdentifier(name, false), type, ColumnMetadata.NO_UNIQUE_ID, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null);
        }
    }

    private class TableRangeSearcher implements RangeSearcher
    {
        private final Index tableIndex;

        private TableRangeSearcher()
        {
            this.tableIndex = cfs.indexManager.getIndexByName("record");
            if (!cfs.indexManager.isIndexQueryable(tableIndex))
                throw new AssertionError("Journal record index is not queryable");
        }

        @Override
        public Result search(int commandStoreId, TokenRange range, TxnId minTxnId, Timestamp maxTxnId)
        {
            CloseableIterator<TxnId> inMemory = index.search(commandStoreId, range, minTxnId, maxTxnId).results();
            CloseableIterator<TxnId> table = tableSearch(commandStoreId, range.start(), range.end());
            return new DefaultResult(minTxnId, maxTxnId, MergeIterator.get(Arrays.asList(inMemory, table)));
        }

        @Override
        public Result search(int commandStoreId, TokenKey key, TxnId minTxnId, Timestamp maxTxnId)
        {
            CloseableIterator<TxnId> inMemory = index.search(commandStoreId, key, minTxnId, maxTxnId).results();
            CloseableIterator<TxnId> table = tableSearch(commandStoreId, key);
            return new DefaultResult(minTxnId, maxTxnId, MergeIterator.get(Arrays.asList(inMemory, table)));
        }

        private CloseableIterator<TxnId> tableSearch(int store, TokenKey start, TokenKey end)
        {
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(AccordJournalTable.SyntheticColumn.participants.metadata, Operator.GT, OrderedRouteSerializer.serialize(start));
            rowFilter.add(AccordJournalTable.SyntheticColumn.participants.metadata, Operator.LTE, OrderedRouteSerializer.serialize(end));
            rowFilter.add(AccordJournalTable.SyntheticColumn.store_id.metadata, Operator.EQ, Int32Type.instance.decompose(store));

            return process(store, rowFilter);
        }

        private CloseableIterator<TxnId> tableSearch(int store, TokenKey key)
        {
            RowFilter rowFilter = RowFilter.create(false);
            rowFilter.add(AccordJournalTable.SyntheticColumn.participants.metadata, Operator.GTE, OrderedRouteSerializer.serialize(key));
            rowFilter.add(AccordJournalTable.SyntheticColumn.participants.metadata, Operator.LTE, OrderedRouteSerializer.serialize(key));
            rowFilter.add(AccordJournalTable.SyntheticColumn.store_id.metadata, Operator.EQ, Int32Type.instance.decompose(store));

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
                        JournalKey partitionKeyComponents = AccordKeyspace.JournalColumns.getJournalKey(next.partitionKey());
                        Invariants.require(partitionKeyComponents.commandStoreId == storeId,
                                              () -> String.format("table index returned a command store other than the exepcted one; expected %d != %d", storeId, partitionKeyComponents.commandStoreId));
                        return partitionKeyComponents.id;
                    }
                };
            }
        }
    }

    /**
     * Perform a read from Journal table, followed by the reads from all journal segments.
     * <p>
     * When reading from journal segments, skip descriptors that were read from the table.
     */
    public void readAll(K key, Reader reader)
    {
        readAll(key, new RecordConsumerAdapter(reader));
    }

    public void readAll(K key, RecordConsumer<K> reader)
    {
        JournalAndTableRecordConsumer consumer = new JournalAndTableRecordConsumer(key, reader);
        journal.readAll(key, consumer);
        consumer.readTable();
    }

    private void readAllFromTable(K key, TableRecordConsumer onEntry)
    {
        DecoratedKey pk = AccordKeyspace.JournalColumns.decorate(key);
        try (RefViewFragment view = cfs.selectAndReference(View.select(SSTableSet.LIVE, pk)))
        {
            if (view.sstables.isEmpty())
                return;

            List<UnfilteredRowIterator> iters = new ArrayList<>(Math.min(4, view.sstables.size()));
            try
            {
                for (SSTableReader sstable : view.sstables)
                {
                    if (!sstable.mayContainAssumingKeyIsInRange(pk))
                        continue;

                    UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs, sstable, pk, Slices.ALL, ColumnFilter.all(cfs.metadata()), false, NOOP_LISTENER);
                    if (iter.getClass() != EmptyIterators.EmptyUnfilteredRowIterator.class)
                        iters.add(iter);
                }

                if (!iters.isEmpty())
                {
                    EntryHolder<K> into = new EntryHolder<>();
                    try (UnfilteredRowIterator iter = UnfilteredRowIterators.merge(iters))
                    {
                        while (iter.hasNext()) readRow(key, iter.next(), into, onEntry);
                    }
                }
            }
            catch (Throwable t)
            {
                String message = "Failed to read from " + iters;
                for (UnfilteredRowIterator iter : iters)
                {
                    try { iter.close(); }
                    catch (Throwable t2) { t.addSuppressed(t2); }
                }
                throw new FSReadError(message, t);
            }
        }
    }

    private void readRow(K key, Unfiltered unfiltered, EntryHolder<K> into, RecordConsumer<K> onEntry)
    {
        Invariants.require(unfiltered.isRow());
        Row row = (Row) unfiltered;

        long descriptor = LongType.instance.compose(ByteBuffer.wrap((byte[]) row.clustering().get(0)));
        int position = Int32Type.instance.compose(ByteBuffer.wrap((byte[]) row.clustering().get(1)));
        into.key = key;
        into.value = row.getCell(recordColumn).buffer();
        into.userVersion = Int32Type.instance.compose(row.getCell(versionColumn).buffer());

        onEntry.accept(descriptor, position, into.key, into.value, into.userVersion);
    }

    @SuppressWarnings("resource") // Auto-closeable iterator will release related resources
    public CloseableIterator<Journal.KeyRefs<K>> keyIterator()
    {
        return new JournalAndTableKeyIterator();
    }

    private class TableIterator extends AbstractIterator<K> implements CloseableIterator<K>
    {
        private final UnfilteredPartitionIterator mergeIterator;
        private final RefViewFragment view;

        private TableIterator()
        {
            view = cfs.selectAndReference(v -> v.select(SSTableSet.LIVE));
            List<ISSTableScanner> scanners = new ArrayList<>();
            for (SSTableReader sstable : view.sstables)
                scanners.add(sstable.getScanner());

            mergeIterator = view.sstables.isEmpty()
                            ? EmptyIterators.unfilteredPartition(cfs.metadata())
                            : UnfilteredPartitionIterators.merge(scanners, UnfilteredPartitionIterators.MergeListener.NOOP);
        }

        @CheckForNull
        protected K computeNext()
        {
            K ret = null;
            if (mergeIterator.hasNext())
            {
                try (UnfilteredRowIterator partition = mergeIterator.next())
                {
                    ret = (K) AccordKeyspace.JournalColumns.getJournalKey(partition.partitionKey());
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

    private class JournalAndTableKeyIterator extends AbstractIterator<Journal.KeyRefs<K>> implements CloseableIterator<Journal.KeyRefs<K>>
    {
        final TableIterator tableIterator;
        final Journal<K, V>.StaticSegmentKeyIterator journalIterator;

        private JournalAndTableKeyIterator()
        {
            this.tableIterator = new TableIterator();
            this.journalIterator = journal.staticSegmentKeyIterator();
        }

        K prevFromTable = null;
        K prevFromJournal = null;

        @Override
        protected Journal.KeyRefs<K> computeNext()
        {
            K tableKey = tableIterator.hasNext() ? tableIterator.peek() : null;
            K journalKey = journalIterator.hasNext() ? journalIterator.peek().key() : null;

            if (journalKey != null)
            {
                Invariants.require(prevFromJournal == null || keySupport.compare(journalKey, prevFromJournal) >= 0, // == for case where we have not consumed previous on prev iteration
                                   "Incorrect sort order in journal segments: %s should strictrly follow %s " + this, journalKey, prevFromJournal);
                prevFromJournal = journalKey;
            }
            else
            {
                prevFromJournal = null;
            }

            if (tableKey != null)
            {
                Invariants.require(prevFromTable == null || keySupport.compare(tableKey, prevFromTable) >= 0, // == for case where we have not consumed previous on prev iteration
                                   "Incorrect sort order in journal table: %s should strictrly follow %s " + this, tableKey, prevFromTable);
                prevFromTable = tableKey;
            }
            else
            {
                prevFromTable = null;
            }

            if (tableKey == null)
                return journalKey == null ? endOfData() : journalIterator.next();

            if (journalKey == null)
                return new Journal.KeyRefs<>(tableIterator.next());

            int cmp = keySupport.compare(tableKey, journalKey);
            if (cmp == 0)
            {
                tableIterator.next();
                return journalIterator.next();
            }

            return cmp > 0 ? new Journal.KeyRefs<>(tableIterator.next()) : journalIterator.next();
        }

        public void close()
        {
            tableIterator.close();
            journalIterator.close();
        }
    }

    public static void readBuffer(ByteBuffer buffer, Reader reader, Version userVersion)
    {
        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
        {
            reader.read(in, userVersion);
        }
        catch (IOException e)
        {
            // can only throw if serializer is buggy or bytes got corrupted
            throw new RuntimeException(e);
        }
    }
}