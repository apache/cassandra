/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import static org.junit.Assert.*;

public class RowAndDeletionMergeIteratorTest
{
    private static final String KEYSPACE1 = "RowTest";
    private static final String CF_STANDARD1 = "Standard1";

    private long nowInSeconds;
    private DecoratedKey dk;
    private ColumnFamilyStore cfs;
    private TableMetadata cfm;
    private ColumnMetadata defA;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        TableMetadata.Builder builder =
            TableMetadata.builder(KEYSPACE1, CF_STANDARD1)
                         .addPartitionKeyColumn("key", AsciiType.instance)
                         .addClusteringColumn("col1", Int32Type.instance)
                         .addRegularColumn("a", Int32Type.instance);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), builder);
    }

    @Before
    public void setup()
    {
        nowInSeconds = FBUtilities.nowInSeconds();
        dk = Util.dk("key0");
        cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        cfm = cfs.metadata();
        defA = cfm.getColumn(new ColumnIdentifier("a", true));
    }

    @Test
    public void testWithNoRangeTombstones()
    {
        Iterator<Row> rowIterator = createRowIterator();
        UnfilteredRowIterator iterator = createMergeIterator(rowIterator, Collections.emptyIterator(), false);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 0);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 1);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 2);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 3);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 4);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithOnlyRangeTombstones()
    {
        long delTime = nowInSeconds + 1L;
        long timestamp = toMillis(delTime);

        Iterator<RangeTombstone> rangeTombstoneIterator = createRangeTombstoneIterator(rt(1, false, 3, false, timestamp, delTime),
                                                                                       atLeast(4, timestamp, delTime));
        UnfilteredRowIterator iterator = createMergeIterator(Collections.emptyIterator(), rangeTombstoneIterator, false);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.EXCL_START_BOUND, 1);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.EXCL_END_BOUND, 3);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.INCL_START_BOUND, 4);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.TOP);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithAtMostRangeTombstone()
    {
        Iterator<Row> rowIterator = createRowIterator();

        long delTime = nowInSeconds + 1L;
        long timestamp = toMillis(delTime);

        Iterator<RangeTombstone> rangeTombstoneIterator = createRangeTombstoneIterator(atMost(0, timestamp, delTime));

        UnfilteredRowIterator iterator = createMergeIterator(rowIterator, rangeTombstoneIterator, false);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.BOTTOM);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.INCL_END_BOUND, 0);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 1);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 2);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 3);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 4);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithGreaterThanRangeTombstone()
    {
        Iterator<Row> rowIterator = createRowIterator();

        long delTime = nowInSeconds + 1L;
        long timestamp = toMillis(delTime);

        Iterator<RangeTombstone> rangeTombstoneIterator = createRangeTombstoneIterator(greaterThan(2, timestamp, delTime));

        UnfilteredRowIterator iterator = createMergeIterator(rowIterator, rangeTombstoneIterator, false);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 0);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 1);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 2);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.EXCL_START_BOUND, 2);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.TOP);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithAtMostAndGreaterThanRangeTombstone()
    {
        Iterator<Row> rowIterator = createRowIterator();

        long delTime = nowInSeconds + 1L;
        long timestamp = toMillis(delTime);

        Iterator<RangeTombstone> rangeTombstoneIterator = createRangeTombstoneIterator(atMost(0, timestamp, delTime),
                                                                                       greaterThan(2, timestamp, delTime));

        UnfilteredRowIterator iterator = createMergeIterator(rowIterator, rangeTombstoneIterator, false);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.BOTTOM);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.INCL_END_BOUND, 0);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 1);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 2);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.EXCL_START_BOUND, 2);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.TOP);

        assertFalse(iterator.hasNext());
    }

    private void assertRtMarker(Unfiltered unfiltered, ClusteringPrefix.Kind kind, int col1)
    {
        assertEquals(Unfiltered.Kind.RANGE_TOMBSTONE_MARKER, unfiltered.kind());
        assertEquals(kind, unfiltered.clustering().kind());
        assertEquals(bb(col1), unfiltered.clustering().get(0));
    }

    @Test
    public void testWithIncludingEndExcludingStartMarker()
    {
        Iterator<Row> rowIterator = createRowIterator();

        long delTime1 = nowInSeconds + 1L;
        long timestamp1 = toMillis(delTime1);
        long delTime2 = delTime1 + 1L;
        long timestamp2 = toMillis(delTime2);

        Iterator<RangeTombstone> rangeTombstoneIterator = createRangeTombstoneIterator(atMost(2, timestamp1, delTime1),
                                                                                       greaterThan(2, timestamp2, delTime2));

        UnfilteredRowIterator iterator = createMergeIterator(rowIterator, rangeTombstoneIterator, false);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.BOTTOM);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY, 2);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.TOP);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithExcludingEndIncludingStartMarker()
    {
        Iterator<Row> rowIterator = createRowIterator();

        long delTime1 = nowInSeconds + 1L;
        long timestamp1 = toMillis(delTime1);
        long delTime2 = delTime1 + 1L;
        long timestamp2 = toMillis(delTime2);

        Iterator<RangeTombstone> rangeTombstoneIterator = createRangeTombstoneIterator(lessThan(2, timestamp1, delTime1),
                                                                                       atLeast(2, timestamp2, delTime2));

        UnfilteredRowIterator iterator = createMergeIterator(rowIterator, rangeTombstoneIterator, false);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.BOTTOM);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY, 2);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.TOP);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNonShadowingTombstone()
    {
        Iterator<Row> rowIterator = createRowIterator();

        Iterator<RangeTombstone> rangeTombstoneIterator = createRangeTombstoneIterator(atMost(0, -1L, 0));

        UnfilteredRowIterator iterator = createMergeIterator(rowIterator, rangeTombstoneIterator, false);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), BufferClusteringBound.BOTTOM);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 0);

        assertTrue(iterator.hasNext());
        assertRtMarker(iterator.next(), ClusteringPrefix.Kind.INCL_END_BOUND, 0);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 1);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 2);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 3);

        assertTrue(iterator.hasNext());
        assertRow(iterator.next(), 4);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithPartitionLevelTombstone()
    {
        Iterator<Row> rowIterator = createRowIterator();

        long delTime = nowInSeconds - 1L;
        long timestamp = toMillis(delTime);

        Iterator<RangeTombstone> rangeTombstoneIterator = createRangeTombstoneIterator(atMost(0, timestamp, delTime),
                                                                                       greaterThan(2, timestamp, delTime));

        long partitionDelTime = nowInSeconds + 1L;
        long partitionTimestamp = toMillis(partitionDelTime);

        UnfilteredRowIterator iterator = createMergeIterator(rowIterator,
                                                             rangeTombstoneIterator,
                                                             DeletionTime.build(partitionTimestamp, partitionDelTime),
                                                             false);

        assertFalse(iterator.hasNext());
    }

    /**
     * RTL doesn't correctly merge range tombstones in some situations (see CASSANDRA-14894)
     */
    @Test
    public void testWithNoopBoundaryMarkers()
    {
        PartitionUpdate update = PartitionUpdate.emptyUpdate(cfm, dk);
        RangeTombstoneList rtl = new RangeTombstoneList(cfm.comparator, 10);
        rtl.add(rt(1, 2, 5, 5));
        rtl.add(rt(3, 4, 5, 5));
        rtl.add(rt(5, 6, 5, 5));
        rtl.add(rt(0, 8, 6, 6)); // <- supersedes all other tombstones

        Assert.assertEquals(3, rtl.size());

        try (UnfilteredRowIterator partition = createMergeIterator(update.iterator(), rtl.iterator(), false))
        {
            assertRtMarker(partition.next(), ClusteringPrefix.Kind.INCL_START_BOUND, 0);
            assertRtMarker(partition.next(), ClusteringPrefix.Kind.INCL_END_BOUND, 8);
            assertFalse(partition.hasNext());
        }
    }

    private void assertRtMarker(Unfiltered unfiltered, ClusteringBoundOrBoundary<?> bound)
    {
        assertEquals(Unfiltered.Kind.RANGE_TOMBSTONE_MARKER, unfiltered.kind());
        assertEquals(bound, unfiltered.clustering());
    }

    private void assertRow(Unfiltered unfiltered, int col1)
    {
        assertEquals(Unfiltered.Kind.ROW, unfiltered.kind());
        assertEquals(cfm.comparator.make(col1), unfiltered.clustering());
    }

    private Iterator<RangeTombstone> createRangeTombstoneIterator(RangeTombstone... tombstones)
    {
        RangeTombstoneList list = new RangeTombstoneList(cfm.comparator, 10);

        for (RangeTombstone tombstone : tombstones)
            list.add(tombstone);

        return list.iterator(Slice.ALL, false);
    }

    private Iterator<Row> createRowIterator()
    {
        PartitionUpdate.Builder update = new PartitionUpdate.Builder(cfm, dk, cfm.regularAndStaticColumns(), 1);
        for (int i = 0; i < 5; i++)
            addRow(update, i, i);

        return update.build().iterator();
    }

    private UnfilteredRowIterator createMergeIterator(Iterator<Row> rows, Iterator<RangeTombstone> tombstones, boolean reversed)
    {
        return createMergeIterator(rows, tombstones, DeletionTime.LIVE, reversed);
    }

    private UnfilteredRowIterator createMergeIterator(Iterator<Row> rows,
                                                      Iterator<RangeTombstone> tombstones,
                                                      DeletionTime deletionTime,
                                                      boolean reversed)
    {
        return new RowAndDeletionMergeIterator(cfm,
                                               Util.dk("k"),
                                               deletionTime,
                                               ColumnFilter.all(cfm),
                                               Rows.EMPTY_STATIC_ROW,
                                               reversed,
                                               EncodingStats.NO_STATS,
                                               rows,
                                               tombstones,
                                               true);
    }

    private void addRow(PartitionUpdate.Builder update, int col1, int a)
    {
        update.add(BTreeRow.singleCellRow(update.metadata().comparator.make(col1), makeCell(defA, a, 0)));
    }

    private Cell<?> makeCell(ColumnMetadata columnMetadata, int value, long timestamp)
    {
        return BufferCell.live(columnMetadata, timestamp, ((AbstractType) columnMetadata.cellValueType()).decompose(value));
    }

    private static RangeTombstone atLeast(int start, long tstamp, long delTime)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(bb(start)), BufferClusteringBound.TOP), DeletionTime.build(tstamp, delTime));
    }

    private static RangeTombstone atMost(int end, long tstamp, long delTime)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.BOTTOM, BufferClusteringBound.inclusiveEndOf(bb(end))), DeletionTime.build(tstamp, delTime));
    }

    private static RangeTombstone lessThan(int end, long tstamp, long delTime)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.BOTTOM, BufferClusteringBound.exclusiveEndOf(bb(end))), DeletionTime.build(tstamp, delTime));
    }

    private static RangeTombstone greaterThan(int start, long tstamp, long delTime)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.exclusiveStartOf(bb(start)), BufferClusteringBound.TOP), DeletionTime.build(tstamp, delTime));
    }

    private static RangeTombstone rt(int start, boolean startInclusive, int end, boolean endInclusive, long tstamp, long delTime)
    {
        ClusteringBound<?> startBound = startInclusive ? BufferClusteringBound.inclusiveStartOf(bb(start)) : BufferClusteringBound.exclusiveStartOf(bb(start));
        ClusteringBound<?> endBound = endInclusive ? BufferClusteringBound.inclusiveEndOf(bb(end)) : BufferClusteringBound.exclusiveEndOf(bb(end));

        return new RangeTombstone(Slice.make(startBound, endBound), DeletionTime.build(tstamp, delTime));
    }

    private static RangeTombstone rt(int start, int end, long tstamp, int delTime)
    {
        return rt(start, true, end, true, tstamp, delTime);
    }

    private static ByteBuffer bb(int i)
    {
        return ByteBufferUtil.bytes(i);
    }

    private long toMillis(long timeInSeconds)
    {
        return timeInSeconds * 1000L;
    }
}
