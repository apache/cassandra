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
package org.apache.cassandra.io.sstable.format;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.db.rows.RangeTombstoneBoundMarker.exclusiveClose;
import static org.apache.cassandra.db.rows.RangeTombstoneBoundMarker.exclusiveOpen;
import static org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen;
import static org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker.inclusiveCloseExclusiveOpen;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableFlushObserverTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        CommitLog.instance.start();
    }

    private static final String KS_NAME = "test";

    private final long now = System.currentTimeMillis();
    private final int nowInSec = FBUtilities.nowInSeconds();

    /**
     * Test {@link SSTableFlushObserver} when the schema doesn't have a clustering key.
     */
    @Test
    public void testWithEmptyClustering()
    {
        TableMetadata metadata = TableMetadata.builder(KS_NAME, "flush_observer")
                                              .addPartitionKeyColumn("id", UTF8Type.instance)
                                              .addRegularColumn("age", Int32Type.instance)
                                              .addRegularColumn("height", LongType.instance)
                                              .addRegularColumn("name", UTF8Type.instance)
                                              .build();

        Map<PartitionHeader, List<Unfiltered>> partitions = new LinkedHashMap<>();

        partitions.put(header(decompose("key1"), new DeletionTime(1, 10), Rows.EMPTY_STATIC_ROW),
                       unfiltereds(row(Clustering.EMPTY,
                                       liveCell(metadata, "age", decompose(27)),
                                       liveCell(metadata, "height", decompose(183L)),
                                       liveCell(metadata, "name", decompose("jack")))));

        partitions.put(header(decompose("key3"), new DeletionTime(2, 20), Rows.EMPTY_STATIC_ROW),
                       unfiltereds(row(Clustering.EMPTY,
                                       liveCell(metadata, "age", decompose(30)),
                                       liveCell(metadata, "height", decompose(178L)),
                                       liveCell(metadata, "name", decompose("ken")))));

        partitions.put(header(decompose("key2"), new DeletionTime(3, 30), Rows.EMPTY_STATIC_ROW),
                       unfiltereds(row(Clustering.EMPTY,
                                       liveCell(metadata, "age", decompose(30)),
                                       liveCell(metadata, "height", decompose(180L)),
                                       liveCell(metadata, "name", decompose("jim")))));

        testFlushObserver(metadata, partitions);
    }

    /**
     * Test {@link SSTableFlushObserver} when the schema has a clustering key.
     */
    @Test
    public void testWithNotEmptyClustering()
    {
        TableMetadata metadata = TableMetadata.builder(KS_NAME, "flush_observer_clustering")
                                              .addPartitionKeyColumn("id", UTF8Type.instance)
                                              .addClusteringColumn("name", UTF8Type.instance)
                                              .addRegularColumn("age", Int32Type.instance)
                                              .addRegularColumn("height", LongType.instance)
                                              .build();

        Map<PartitionHeader, List<Unfiltered>> partitions = new LinkedHashMap<>();

        partitions.put(header(decompose("key1"), new DeletionTime(1, 10), Rows.EMPTY_STATIC_ROW),
                       unfiltereds(row(clustering(decompose("kim")),
                                       liveCell(metadata, "age", decompose(27)),
                                       liveCell(metadata, "height", decompose(183L))),
                                   row(clustering(decompose("jim")),
                                       liveCell(metadata, "age", decompose(27)),
                                       liveCell(metadata, "height", decompose(183L))),
                                   row(clustering(decompose("tim")),
                                       liveCell(metadata, "age", decompose(54)),
                                       liveCell(metadata, "height", decompose(181L)))));

        partitions.put(header(decompose("key2"), new DeletionTime(2, 20), Rows.EMPTY_STATIC_ROW),
                       unfiltereds(row(clustering(decompose("kim")),
                                       liveCell(metadata, "age", decompose(36)),
                                       liveCell(metadata, "height", decompose(172L))),
                                   row(clustering(decompose("jim")),
                                       liveCell(metadata, "age", decompose(30)),
                                       liveCell(metadata, "height", decompose(178L))),
                                   row(clustering(decompose("tom")),
                                       liveCell(metadata, "age", decompose(22)),
                                       liveCell(metadata, "height", decompose(164L)))));

        testFlushObserver(metadata, partitions);
    }

    /**
     * Test {@link SSTableFlushObserver} when the schema has static rows.
     */
    @Test
    public void testWithStaticRow()
    {
        TableMetadata metadata = TableMetadata.builder(KS_NAME, "flush_observer_static")
                                              .addPartitionKeyColumn("id", UTF8Type.instance)
                                              .addClusteringColumn("name", UTF8Type.instance)
                                              .addStaticColumn("static_1", UTF8Type.instance)
                                              .addStaticColumn("static_2", UTF8Type.instance)
                                              .addRegularColumn("age", Int32Type.instance)
                                              .addRegularColumn("height", LongType.instance)
                                              .build();

        Map<PartitionHeader, List<Unfiltered>> partitions = new LinkedHashMap<>();

        partitions.put(header(decompose("key0"), new DeletionTime(1, 10),
                              staticRow(liveCell(metadata, "static_1", decompose("static_1_0")))),
                       unfiltereds());

        partitions.put(header(decompose("key1"), new DeletionTime(2, 20),
                              staticRow(liveCell(metadata, "static_2", decompose("static_2_1")))),
                       unfiltereds());

        partitions.put(header(decompose("key4"), new DeletionTime(3, 30),
                              staticRow(liveCell(metadata, "static_1", decompose("static_1_4")),
                                        liveCell(metadata, "static_2", decompose("static_2_4")))),
                       unfiltereds());

        partitions.put(header(decompose("key3"), new DeletionTime(4, 40), staticRow()),
                       unfiltereds(row(clustering(decompose("bob")),
                                       liveCell(metadata, "age", decompose(36)),
                                       liveCell(metadata, "height", decompose(172L))),
                                   row(clustering(decompose("ron")),
                                       liveCell(metadata, "age", decompose(41)),
                                       liveCell(metadata, "height", decompose(183L)))));

        partitions.put(header(decompose("key2"), new DeletionTime(5, 50),
                              staticRow(liveCell(metadata, "static_1", decompose("static_1_2")),
                                        liveCell(metadata, "static_2", decompose("static_2_2")))),
                       unfiltereds(row(clustering(decompose("kim")),
                                       liveCell(metadata, "age", decompose(27)),
                                       liveCell(metadata, "height", decompose(183L))),
                                   row(clustering(decompose("tim")),
                                       liveCell(metadata, "age", decompose(24)),
                                       liveCell(metadata, "height", decompose(165L)))));

        testFlushObserver(metadata, partitions);
    }

    /**
     * Test {@link SSTableFlushObserver} with tombstones.
     */
    @Test
    public void testWithTombstones()
    {
        TableMetadata metadata = TableMetadata.builder(KS_NAME, "flush_observer_tombstones")
                                              .addPartitionKeyColumn("id", UTF8Type.instance)
                                              .addRegularColumn("age", Int32Type.instance)
                                              .addRegularColumn("height", LongType.instance)
                                              .addRegularColumn("name", UTF8Type.instance)
                                              .build();

        Map<PartitionHeader, List<Unfiltered>> partitions = new LinkedHashMap<>();

        partitions.put(header(decompose("key1"), new DeletionTime(1, 10), Rows.EMPTY_STATIC_ROW),
                       unfiltereds(row(Clustering.EMPTY,
                                       tombstone(metadata, "age"),
                                       liveCell(metadata, "height", decompose(183L)),
                                       liveCell(metadata, "name", decompose("jack")))));

        partitions.put(header(decompose("key3"), new DeletionTime(2, 20), Rows.EMPTY_STATIC_ROW),
                       unfiltereds(row(Clustering.EMPTY,
                                       liveCell(metadata, "age", decompose(30)),
                                       liveCell(metadata, "height", decompose(178L)),
                                       tombstone(metadata, "name"))));

        partitions.put(header(decompose("key2"), new DeletionTime(3, 30), Rows.EMPTY_STATIC_ROW),
                       unfiltereds(row(Clustering.EMPTY,
                                       tombstone(metadata, "age"),
                                       tombstone(metadata, "height"),
                                       tombstone(metadata, "name"))));

        testFlushObserver(metadata, partitions);
    }

    /**
     * Test {@link SSTableFlushObserver} with {@link RangeTombstoneMarker}.
     */
    @Test
    public void testWithRangeTombstoneMarkers()
    {
        TableMetadata metadata = TableMetadata.builder(KS_NAME, "flush_observer_range_tombstone_markers")
                                              .addPartitionKeyColumn("id", UTF8Type.instance)
                                              .addClusteringColumn("name", UTF8Type.instance)
                                              .addRegularColumn("age", Int32Type.instance)
                                              .build();

        DeletionTime dt = new DeletionTime(now, nowInSec);
        Map<PartitionHeader, List<Unfiltered>> partitions = new LinkedHashMap<>();

        partitions.put(header(decompose("key1"), dt, Rows.EMPTY_STATIC_ROW),
                       unfiltereds(exclusiveOpen(false, Clustering.make(decompose("alice")), dt),
                                   exclusiveClose(false, Clustering.make(decompose("bob")), dt),
                                   exclusiveCloseInclusiveOpen(false, Clustering.make(decompose("carol")), dt, dt),
                                   inclusiveCloseExclusiveOpen(false, Clustering.make(decompose("dan")), dt, dt)));

        partitions.put(header(decompose("key2"), dt, Rows.EMPTY_STATIC_ROW),
                       unfiltereds(exclusiveOpen(true, Clustering.make(decompose("alice")), dt),
                                   exclusiveClose(true, Clustering.make(decompose("bob")), dt),
                                   exclusiveCloseInclusiveOpen(true, Clustering.make(decompose("carol")), dt, dt),
                                   inclusiveCloseExclusiveOpen(true, Clustering.make(decompose("dan")), dt, dt)));

        testFlushObserver(metadata, partitions);
    }

    private static void testFlushObserver(TableMetadata metadata, Map<PartitionHeader, List<Unfiltered>> partitions)
    {
        for (SSTableFormat.Type type : SSTableFormat.Type.values())
            testFlushObserver(type, metadata, partitions);
    }

    private static void testFlushObserver(SSTableFormat.Type type,
                                          TableMetadata metadata,
                                          Map<PartitionHeader, List<Unfiltered>> partitions)
    {
        LifecycleTransaction transaction = LifecycleTransaction.offline(OperationType.COMPACTION);
        FlushObserver observer = new FlushObserver();

        String sstableDirectory = DatabaseDescriptor.getAllDataFileLocations()[0];
        File directory = new File(sstableDirectory, metadata.keyspace + File.separator + metadata.name);
        directory.deleteOnExit();

        if (!directory.exists() && !directory.mkdirs())
            throw new FSWriteError(new IOException("failed to create tmp directory"), directory);

        SSTableFormat format = type.info;
        Descriptor descriptor = new Descriptor(format.getLatestVersion(),
                                               directory,
                                               metadata.keyspace,
                                               metadata.name,
                                               new SequenceBasedSSTableId(0),
                                               type);

        SSTableWriter writer = format.getWriterFactory()
                                     .open(descriptor,
                                           10L, 0L, null, false, TableMetadataRef.forOfflineTools(metadata),
                                           new MetadataCollector(metadata.comparator).sstableLevel(0),
                                           new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS),
                                           Collections.singletonList(observer),
                                           transaction,
                                           Collections.emptySet());

        SSTableReader reader;
        try
        {
            partitions.forEach((key, rows) -> writer.append(new RowIterator(metadata, key, rows)));
            reader = writer.finish(true);
        }
        finally
        {
            FileUtils.closeQuietly(writer);
        }

        assertTrue(observer.isComplete);
        assertEquals(partitions.size(), observer.headers.size());
        assertEquals(partitions.values().stream().mapToInt(List::size).sum(), observer.unfiltereds.size());

        ColumnFilter columnFilter = ColumnFilter.all(metadata);

        try
        {
            for (FlushObserver.HeaderEntry e : observer.headers)
            {
                try (RandomAccessReader in = reader.openKeyComponentReader())
                {
                    assertEquals(e.key, reader.keyAt(in, e.keyPosition));
                    assertEquals(e.deletionTime, reader.partitionLevelDeletionAt(e.deletionTimePosition));
                    assertEquals(e.staticRow, reader.staticRowAt(e.staticRowPosition, columnFilter));
                }
            }

            for (FlushObserver.UnfilteredEntry e : observer.unfiltereds)
            {
                try (RandomAccessReader in = reader.openKeyComponentReader())
                {
                    assertEquals(e.key, reader.keyAt(in, e.keyPosition));
                    assertEquals(e.unfiltered.clustering(), reader.clusteringAt(e.unfilteredPosition));
                    assertEquals(e.unfiltered, reader.unfilteredAt(e.unfilteredPosition, columnFilter));
                }
            }
        }
        catch (IOException ex)
        {
            throw new FSReadError(ex, reader.getFilename());
        }
    }

    private static class RowIterator extends AbstractUnfilteredRowIterator
    {
        private final Iterator<Unfiltered> unfiltereds;

        private RowIterator(TableMetadata metadata, PartitionHeader header, Collection<Unfiltered> unfiltereds)
        {
            super(metadata,
                  header.key,
                  header.deletionTime,
                  metadata.regularAndStaticColumns(),
                  header.staticRow,
                  false,
                  EncodingStats.NO_STATS);
            this.unfiltereds = unfiltereds.iterator();
        }

        @Override
        protected Unfiltered computeNext()
        {
            return unfiltereds.hasNext() ? unfiltereds.next() : endOfData();
        }
    }

    private static class FlushObserver implements SSTableFlushObserver
    {
        private final List<HeaderEntry> headers = new ArrayList<>();
        private final List<UnfilteredEntry> unfiltereds = new ArrayList<>();

        private boolean pendingHeader;
        private DecoratedKey currentKey;
        private long currentKeyPosition;
        private DeletionTime currentDeletionTime;
        private long currentDeletionTimePosition;
        private Row currentStaticRow = Rows.EMPTY_STATIC_ROW;
        private long currentStaticRowPosition;
        private boolean isComplete;

        @Override
        public void begin()
        {
            pendingHeader = false;
        }

        @Override
        public void startPartition(DecoratedKey key, long position)
        {
            currentKey = key;
            currentKeyPosition = position;

            if (pendingHeader)
                headers.add(new HeaderEntry());
            pendingHeader = true;
        }

        @Override
        public void partitionLevelDeletion(DeletionTime deletionTime, long position)
        {
            currentDeletionTime = deletionTime;
            currentDeletionTimePosition = position;
        }

        @Override
        public void staticRow(Row staticRow, long position)
        {
            currentStaticRow = staticRow;
            currentStaticRowPosition = position;
        }

        @Override
        public void nextUnfilteredCluster(Unfiltered unfiltered, long position)
        {
            unfiltereds.add(new UnfilteredEntry(unfiltered, position));
        }

        @Override
        public void complete()
        {
            isComplete = true;
            if (pendingHeader)
                headers.add(new HeaderEntry());
        }

        class HeaderEntry
        {
            final DecoratedKey key;
            final long keyPosition;
            final DeletionTime deletionTime;
            final long deletionTimePosition;
            final Row staticRow;
            final long staticRowPosition;

            HeaderEntry()
            {
                this.key = currentKey;
                this.keyPosition = currentKeyPosition;
                this.deletionTime = currentDeletionTime;
                this.deletionTimePosition = currentDeletionTimePosition;
                this.staticRow = currentStaticRow;
                this.staticRowPosition = currentStaticRowPosition;
            }
        }

        class UnfilteredEntry
        {
            final DecoratedKey key;
            final long keyPosition;
            final long unfilteredPosition;
            final Unfiltered unfiltered;

            UnfilteredEntry(Unfiltered unfiltered, long unfilteredPosition)
            {
                assertTrue(!unfiltered.isRow() || !((Row) unfiltered).isStatic());
                this.key = currentKey;
                this.keyPosition = currentKeyPosition;
                this.unfilteredPosition = unfilteredPosition;
                this.unfiltered = unfiltered;
            }
        }
    }

    private static class PartitionHeader
    {
        final DecoratedKey key;
        final DeletionTime deletionTime;
        final Row staticRow;

        PartitionHeader(DecoratedKey key, DeletionTime deletionTime, Row staticRow)
        {
            this.key = key;
            this.deletionTime = deletionTime;
            this.staticRow = staticRow;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionHeader that = (PartitionHeader) o;
            return Objects.equals(key, that.key) &&
                   Objects.equals(deletionTime, that.deletionTime) &&
                   Objects.equals(staticRow, that.staticRow);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, deletionTime, staticRow);
        }
    }

    private PartitionHeader header(ByteBuffer key, DeletionTime deletionTime, Row staticRow)
    {
        return new PartitionHeader(DatabaseDescriptor.getPartitioner().decorateKey(key), deletionTime, staticRow);
    }

    private BufferCell liveCell(TableMetadata metadata, String name, ByteBuffer value)
    {
        return BufferCell.live(metadata.getColumn(decompose(name)), now, value);
    }

    private BufferCell tombstone(TableMetadata metadata, String name)
    {
        return BufferCell.tombstone(metadata.getColumn(decompose(name)), now, nowInSec);
    }

    private static Clustering clustering(ByteBuffer... values)
    {
        return Clustering.make(values);
    }

    private static Row row(Clustering clustering, Cell... cells)
    {
        Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        rowBuilder.newRow(clustering);
        for (Cell cell : cells)
            rowBuilder.addCell(cell);
        return rowBuilder.build();
    }

    private static Row staticRow(Cell... cells)
    {
        return row(Clustering.STATIC_CLUSTERING, cells);
    }

    private static List<Unfiltered> unfiltereds(Unfiltered... unfiltereds)
    {
        return Arrays.asList(unfiltereds);
    }

    private static ByteBuffer decompose(String s)
    {
        return UTF8Type.instance.decompose(s);
    }

    private static ByteBuffer decompose(int s)
    {
        return Int32Type.instance.decompose(s);
    }

    private static ByteBuffer decompose(long s)
    {
        return LongType.instance.decompose(s);
    }
}
