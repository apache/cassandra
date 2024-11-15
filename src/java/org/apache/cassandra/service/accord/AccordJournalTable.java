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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import accord.utils.Invariants;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.LongHashSet;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyStore.RefViewFragment;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.StorageHook;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.journal.EntrySerializer.EntryHolder;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.RecordConsumer;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.io.sstable.SSTableReadsListener.NOOP_LISTENER;

public class AccordJournalTable<K extends JournalKey, V>
{
    private static final IntHashSet SENTINEL_HOSTS = new IntHashSet();

    private final Journal<K, V> journal;
    private final ColumnFamilyStore cfs;

    private final ColumnMetadata recordColumn;
    private final ColumnMetadata versionColumn;

    private final KeySupport<K> keySupport;
    private final int accordJournalVersion;

    public AccordJournalTable(Journal<K, V> journal, KeySupport<K> keySupport, ColumnFamilyStore cfs, int accordJournalVersion)
    {
        this.journal = journal;
        this.cfs = cfs;
        this.recordColumn = cfs.metadata().getColumn(ColumnIdentifier.getInterned("record", false));
        this.versionColumn = cfs.metadata().getColumn(ColumnIdentifier.getInterned("user_version", false));
        this.keySupport = keySupport;
        this.accordJournalVersion = accordJournalVersion;
    }

    public void forceCompaction()
    {
        cfs.forceMajorCompaction();
    }

    public interface Reader
    {
        void read(DataInputPlus input, int userVersion) throws IOException;
    }

    private abstract class AbstractRecordConsumer implements RecordConsumer<K>
    {
        protected final Reader reader;

        AbstractRecordConsumer(Reader reader)
        {
            this.reader = reader;
        }

        @Override
        public void accept(long segment, int position, K key, ByteBuffer buffer, IntHashSet hosts, int userVersion)
        {
            readBuffer(buffer, reader, userVersion);
        }
    }

    private class TableRecordConsumer extends AbstractRecordConsumer
    {
        protected LongHashSet visited = null;

        TableRecordConsumer(Reader reader)
        {
            super(reader);
        }

        void visit(long segment)
        {
            if (visited == null)
                visited = new LongHashSet();
            visited.add(segment);
        }

        boolean visited(long segment)
        {
            return visited != null && visited.contains(segment);
        }

        @Override
        public void accept(long segment, int position, K key, ByteBuffer buffer, IntHashSet hosts, int userVersion)
        {
            visit(segment);
            super.accept(segment, position, key, buffer, hosts, userVersion);
        }
    }

    private class JournalAndTableRecordConsumer extends AbstractRecordConsumer
    {
        private final K key;
        private final TableRecordConsumer tableRecordConsumer;

        JournalAndTableRecordConsumer(K key, Reader reader)
        {
            super(reader);
            this.key = key;
            this.tableRecordConsumer = new TableRecordConsumer(reader);
        }

        void readTable()
        {
            readAllFromTable(key, tableRecordConsumer);
        }

        @Override
        public void accept(long segment, int position, K key, ByteBuffer buffer, IntHashSet hosts, int userVersion)
        {
            if (!tableRecordConsumer.visited(segment))
                super.accept(segment, position, key, buffer, hosts, userVersion);
        }
    }

    /**
     * Perform a read from Journal table, followed by the reads from all journal segments.
     * <p>
     * When reading from journal segments, skip descriptors that were read from the table.
     */
    public void readAll(K key, Reader reader)
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

            List<UnfilteredRowIterator> iters = new ArrayList<>(view.sstables.size());
            for (SSTableReader sstable : view.sstables)
                if (sstable.mayContainAssumingKeyIsInRange(pk))
                    iters.add(StorageHook.instance.makeRowIterator(cfs, sstable, pk, Slices.ALL, ColumnFilter.all(cfs.metadata()), false, NOOP_LISTENER));

            if (!iters.isEmpty())
            {
                EntryHolder<K> into = new EntryHolder<>();
                try (UnfilteredRowIterator iter = UnfilteredRowIterators.merge(iters))
                {
                    while (iter.hasNext()) readRow(key, iter.next(), into, onEntry);
                }
            }
        }
    }

    private void readRow(K key, Unfiltered unfiltered, EntryHolder<K> into, RecordConsumer<K> onEntry)
    {
        Invariants.checkState(unfiltered.isRow());
        Row row = (Row) unfiltered;

        long descriptor = LongType.instance.compose(ByteBuffer.wrap((byte[]) row.clustering().get(0)));
        int position = Int32Type.instance.compose(ByteBuffer.wrap((byte[]) row.clustering().get(1)));

        into.key = key;
        into.value = row.getCell(recordColumn).buffer();
        into.hosts = SENTINEL_HOSTS;
        into.userVersion = Int32Type.instance.compose(row.getCell(versionColumn).buffer());

        onEntry.accept(descriptor, position, into.key, into.value, into.hosts, into.userVersion);
    }

    @SuppressWarnings("resource") // Auto-closeable iterator will release related resources
    public KeyOrderIterator<K> readAll()
    {
        return new JournalAndTableKeyIterator();
    }

    private class TableIterator implements Closeable
    {
        private final UnfilteredPartitionIterator mergeIterator;
        private final RefViewFragment view;

        private UnfilteredRowIterator partition;
        private LongHashSet visited = null;

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

        public JournalKey key()
        {
            if (partition == null)
            {
                if (mergeIterator.hasNext())
                    partition = mergeIterator.next();
                else
                    return null;
            }

            return AccordKeyspace.JournalColumns.getJournalKey(partition.partitionKey());
        }

        protected void readAllForKey(K key, RecordConsumer<K> recordConsumer)
        {
            while (partition.hasNext())
            {
                EntryHolder<K> into = new EntryHolder<>();
                // TODO: use flyweight to avoid allocating extra lambdas?
                readRow(key, partition.next(), into, (segment, position, key1, buffer, hosts, userVersion) -> {
                    visit(segment);
                    recordConsumer.accept(segment, position, key1, buffer, hosts, userVersion);
                });
            }

            partition = null;
        }

        void visit(long segment)
        {
            if (visited == null)
                visited = new LongHashSet();
            visited.add(segment);
        }

        boolean visited(long segment)
        {
            return visited != null && visited.contains(segment);
        }


        void clear()
        {
            visited = null;
        }


        @Override
        public void close()
        {
            mergeIterator.close();
            view.close();
        }
    }

    private class JournalAndTableKeyIterator implements KeyOrderIterator<K>
    {
        final TableIterator tableIterator;
        final Journal<K, V>.StaticSegmentIterator staticSegmentIterator;

        private JournalAndTableKeyIterator()
        {
            this.tableIterator = new TableIterator();
            this.staticSegmentIterator = journal.staticSegmentIterator();
        }

        @Override
        public K key()
        {
            // TODO (expected): fix generics mismatch here
            K tableKey = (K)tableIterator.key();
            K journalKey = staticSegmentIterator.key();
            if (tableKey == null)
                return journalKey;
            if (journalKey == null || keySupport.compare(tableKey, journalKey) > 0)
                return journalKey;

            return tableKey;
        }

        @Override
        public void readAllForKey(K key, RecordConsumer<K> reader)
        {
            K tableKey = (K)tableIterator.key();
            K journalKey = staticSegmentIterator.key();
            if (journalKey != null && keySupport.compare(journalKey, key) == 0)
                staticSegmentIterator.readAllForKey(key, (segment, position, key1, buffer, hosts, userVersion) -> {
                    if (!tableIterator.visited(segment))
                        reader.accept(segment, position, key1, buffer, hosts, userVersion);
                });

            if (tableKey != null && keySupport.compare(tableKey, key) == 0)
                tableIterator.readAllForKey(key, reader);

            tableIterator.clear();
        }

        public void close()
        {
            tableIterator.close();
            staticSegmentIterator.close();
        }
    }

    public interface KeyOrderIterator<K> extends Closeable
    {
        K key();
        void readAllForKey(K key, RecordConsumer<K> reader);
        void close();
    }

    public static void readBuffer(ByteBuffer buffer, Reader reader, int userVersion)
    {
        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
        {
            reader.read(in, userVersion);
        }
        catch (IOException e)
        {
            // can only throw if serializer is buggy
            throw new RuntimeException(e);
        }
    }
}