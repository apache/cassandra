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

package org.apache.cassandra.db.rows;

import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.Iterators;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

public class ThrottledUnfilteredIteratorTest extends CQLTester
{
    private static final String KSNAME = "ThrottledUnfilteredIteratorTest";
    private static final String CFNAME = "StandardInteger1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KSNAME,
                                    KeyspaceParams.simple(1),
                                    standardCFMD(KSNAME, CFNAME, 1, UTF8Type.instance, Int32Type.instance, Int32Type.instance));
    }

    static final TableMetadata metadata;
    static final ColumnMetadata v1Metadata;
    static final ColumnMetadata v2Metadata;
    static final ColumnMetadata staticMetadata;

    static
    {
        metadata = TableMetadata.builder("", "")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck1", Int32Type.instance)
                                .addClusteringColumn("ck2", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("v2", Int32Type.instance)
                                .addStaticColumn("s1", Int32Type.instance)
                                .build();
        v1Metadata = metadata.regularAndStaticColumns().columns(false).getSimple(0);
        v2Metadata = metadata.regularAndStaticColumns().columns(false).getSimple(1);
        staticMetadata = metadata.regularAndStaticColumns().columns(true).getSimple(0);
    }

    @Test
    public void emptyPartitionDeletionTest() throws Throwable
    {
        // create cell tombstone, range tombstone, partition deletion
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v1 int, v2 int, PRIMARY KEY (pk, ck1, ck2))");
        // partition deletion
        execute("DELETE FROM %s USING TIMESTAMP 160 WHERE pk=1");

        // flush and generate 1 sstable
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        Util.flush(cfs);
        cfs.disableAutoCompaction();
        cfs.forceMajorCompaction();

        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader reader = cfs.getLiveSSTables().iterator().next();

        try (ISSTableScanner scanner = reader.getScanner();
                CloseableIterator<UnfilteredRowIterator> throttled = ThrottledUnfilteredIterator.throttle(scanner, 100))
        {
            assertTrue(throttled.hasNext());
            UnfilteredRowIterator iterator = throttled.next();
            assertFalse(throttled.hasNext());
            assertFalse(iterator.hasNext());
            assertEquals(iterator.partitionLevelDeletion().markedForDeleteAt(), 160);
        }

        // test opt out
        try (ISSTableScanner scanner = reader.getScanner();
                CloseableIterator<UnfilteredRowIterator> throttled = ThrottledUnfilteredIterator.throttle(scanner, 0))
        {
            assertEquals(scanner, throttled);
        }
    }

    @Test
    public void emptyStaticTest() throws Throwable
    {
        // create cell tombstone, range tombstone, partition deletion
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v1 int, v2 int static, PRIMARY KEY (pk, ck1, ck2))");
        // partition deletion
        execute("UPDATE %s SET v2 = 160 WHERE pk = 1");

        // flush and generate 1 sstable
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        Util.flush(cfs);
        cfs.disableAutoCompaction();
        cfs.forceMajorCompaction();

        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader reader = cfs.getLiveSSTables().iterator().next();

        try (ISSTableScanner scanner = reader.getScanner();
             CloseableIterator<UnfilteredRowIterator> throttled = ThrottledUnfilteredIterator.throttle(scanner, 100))
        {
            assertTrue(throttled.hasNext());
            UnfilteredRowIterator iterator = throttled.next();
            assertFalse(throttled.hasNext());
            assertFalse(iterator.hasNext());
            assertEquals(Int32Type.instance.getSerializer().deserialize(iterator.staticRow().cells().iterator().next().buffer()), Integer.valueOf(160));
        }

        // test opt out
        try (ISSTableScanner scanner = reader.getScanner();
             CloseableIterator<UnfilteredRowIterator> throttled = ThrottledUnfilteredIterator.throttle(scanner, 0))
        {
            assertEquals(scanner, throttled);
        }
    }

    @Test
    public void complexThrottleWithTombstoneTest() throws Throwable
    {
        // create cell tombstone, range tombstone, partition deletion
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v1 int, v2 int, PRIMARY KEY (pk, ck1, ck2))");

        for (int ck1 = 1; ck1 <= 150; ck1++)
            for (int ck2 = 1; ck2 <= 150; ck2++)
            {
                int timestamp = ck1, v1 = ck1, v2 = ck2;
                execute("INSERT INTO %s(pk,ck1,ck2,v1,v2) VALUES(1,?,?,?,?) using timestamp "
                        + timestamp, ck1, ck2, v1, v2);
            }

        for (int ck1 = 1; ck1 <= 100; ck1++)
            for (int ck2 = 1; ck2 <= 100; ck2++)
            {
                if (ck1 % 2 == 0 || ck1 % 3 == 0) // range tombstone
                    execute("DELETE FROM %s USING TIMESTAMP 170 WHERE pk=1 AND ck1=?", ck1);
                else if (ck1 == ck2) // row tombstone
                    execute("DELETE FROM %s USING TIMESTAMP 180 WHERE pk=1 AND ck1=? AND ck2=?", ck1, ck2);
                else if (ck1 == ck2 - 1) // cell tombstone
                    execute("DELETE v2 FROM %s USING TIMESTAMP 190 WHERE pk=1 AND ck1=? AND ck2=?", ck1, ck2);
            }

        // range deletion
        execute("DELETE FROM %s USING TIMESTAMP 150 WHERE pk=1 AND ck1 > 100 AND ck1 < 120");
        execute("DELETE FROM %s USING TIMESTAMP 150 WHERE pk=1 AND ck1 = 50 AND ck2 < 120");
        // partition deletion
        execute("DELETE FROM %s USING TIMESTAMP 160 WHERE pk=1");

        // flush and generate 1 sstable
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        Util.flush(cfs);
        cfs.disableAutoCompaction();
        cfs.forceMajorCompaction();

        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader reader = cfs.getLiveSSTables().iterator().next();

        try (ISSTableScanner scanner = reader.getScanner())
        {
            try (UnfilteredRowIterator rowIterator = scanner.next())
            {
                // only 1 partition data
                assertFalse(scanner.hasNext());
                List<Unfiltered> expectedUnfiltereds = new ArrayList<>();
                rowIterator.forEachRemaining(expectedUnfiltereds::add);

                // test different throttle
                for (Integer throttle : Arrays.asList(2, 3, 4, 5, 11, 41, 99, 1000, 10001))
                {
                    try (ISSTableScanner scannerForThrottle = reader.getScanner())
                    {
                        assertTrue(scannerForThrottle.hasNext());
                        try (UnfilteredRowIterator rowIteratorForThrottle = scannerForThrottle.next())
                        {
                            assertFalse(scannerForThrottle.hasNext());
                            verifyThrottleIterator(expectedUnfiltereds,
                                                   rowIteratorForThrottle,
                                                   new ThrottledUnfilteredIterator(rowIteratorForThrottle, throttle),
                                                   throttle);
                        }
                    }
                }
            }
        }
    }

    private void verifyThrottleIterator(List<Unfiltered> expectedUnfiltereds,
                                        UnfilteredRowIterator rowIteratorForThrottle,
                                        ThrottledUnfilteredIterator throttledIterator,
                                        int throttle)
    {
        List<Unfiltered> output = new ArrayList<>();

        boolean isRevered = rowIteratorForThrottle.isReverseOrder();
        boolean isFirst = true;

        while (throttledIterator.hasNext())
        {
            UnfilteredRowIterator splittedIterator = throttledIterator.next();
            assertMetadata(rowIteratorForThrottle, splittedIterator, isFirst);

            List<Unfiltered> splittedUnfiltereds = new ArrayList<>();

            splittedIterator.forEachRemaining(splittedUnfiltereds::add);

            int remain = expectedUnfiltereds.size() - output.size();
            int expectedSize = remain >= throttle ? throttle : remain;
            if (splittedUnfiltereds.size() != expectedSize)
            {
                assertEquals(expectedSize + 1, splittedUnfiltereds.size());
                // the extra unfilter must be close bound marker
                Unfiltered last = splittedUnfiltereds.get(expectedSize);
                assertTrue(last.isRangeTombstoneMarker());
                RangeTombstoneMarker marker = (RangeTombstoneMarker) last;
                assertFalse(marker.isBoundary());
                assertTrue(marker.isClose(isRevered));
            }
            output.addAll(splittedUnfiltereds);
            if (isFirst)
                isFirst = false;
        }
        int index = 0;
        RangeTombstoneMarker openMarker = null;
        for (int i = 0; i < expectedUnfiltereds.size(); i++)
        {
            Unfiltered expected = expectedUnfiltereds.get(i);
            Unfiltered data = output.get(i);

            // verify that all tombstone are paired
            if (data.isRangeTombstoneMarker())
            {
                RangeTombstoneMarker marker = (RangeTombstoneMarker) data;
                if (marker.isClose(isRevered))
                {
                    assertNotNull(openMarker);
                    openMarker = null;
                }
                if (marker.isOpen(isRevered))
                {
                    assertNull(openMarker);
                    openMarker = marker;
                }
            }
            if (expected.equals(data))
            {
                index++;
            }
            else // because of created closeMarker and openMarker
            {
                assertNotNull(openMarker);
                DeletionTime openDeletionTime = openMarker.openDeletionTime(isRevered);
                // only boundary or row will create extra closeMarker and openMarker
                if (expected.isRangeTombstoneMarker())
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) expected;
                    assertTrue(marker.isBoundary());
                    RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                    assertEquals(boundary.createCorrespondingCloseMarker(isRevered), data);
                    assertEquals(boundary.createCorrespondingOpenMarker(isRevered), output.get(index + 1));
                    assertEquals(openDeletionTime, boundary.endDeletionTime());

                    openMarker = boundary.createCorrespondingOpenMarker(isRevered);
                }
                else
                {
                    RangeTombstoneBoundMarker closeMarker = RangeTombstoneBoundMarker.exclusiveClose(isRevered,
                                                                                                     expected.clustering(),
                                                                                                     openDeletionTime);

                    RangeTombstoneBoundMarker nextOpenMarker = RangeTombstoneBoundMarker.inclusiveOpen(isRevered,
                                                                                                       expected.clustering(),
                                                                                                       openDeletionTime);
                    assertEquals(closeMarker, data);
                    assertEquals(nextOpenMarker, output.get(index + 1));

                    openMarker = nextOpenMarker;
                }
                index += 2;
            }
        }
        assertNull(openMarker);
        assertEquals(output.size(), index);
    }

    @Test
    public void simpleThrottleTest()
    {
        simpleThrottleTest(false);
    }

    @Test
    public void skipTest()
    {
        simpleThrottleTest(true);
    }

    public void simpleThrottleTest(boolean skipOdd)
    {
        // all live rows with partition deletion
        ThrottledUnfilteredIterator throttledIterator;
        UnfilteredRowIterator origin;

        List<Row> rows = new ArrayList<>();
        int rowCount = 1111;

        for (int i = 0; i < rowCount; i++)
            rows.add(createRow(i, createCell(v1Metadata, i), createCell(v2Metadata, i)));

        // testing different throttle limit
        for (int throttle = 2; throttle < 1200; throttle += 21)
        {
            origin = rows(metadata.regularAndStaticColumns(),
                          1,
                          DeletionTime.build(0, 100),
                          createStaticRow(createCell(staticMetadata, 160)),
                          rows.toArray(new Row[0]));
            throttledIterator = new ThrottledUnfilteredIterator(origin, throttle);

            int splittedCount = (int) Math.ceil(rowCount*1.0/throttle);
            for (int i = 1; i <= splittedCount; i++)
            {
                UnfilteredRowIterator splitted = throttledIterator.next();
                assertMetadata(origin, splitted, i == 1);
                // no op
                splitted.close();

                int start = (i - 1) * throttle;
                int end = i == splittedCount ? rowCount : i * throttle;
                if (skipOdd && (i % 2) == 0)
                {
                    assertRows(splitted, rows.subList(start, end).toArray(new Row[0]));
                }
            }
            assertTrue(!throttledIterator.hasNext());
        }
    }

    @Test
    public void throttledPartitionIteratorTest()
    {
        // all live rows with partition deletion
        CloseableIterator<UnfilteredRowIterator> throttledIterator;
        UnfilteredPartitionIterator origin;

        SortedMap<Integer, List<Row>> partitions = new TreeMap<>();
        int partitionCount = 13;
        int baseRowsPerPartition = 1111;

        for (int i = 1; i <= partitionCount; i++)
        {
            ArrayList<Row> rows = new ArrayList<>();
            for (int j = 0; j < (baseRowsPerPartition + i); j++)
                rows.add(createRow(i, createCell(v1Metadata, j), createCell(v2Metadata, j)));
            partitions.put(i, rows);
        }

        // testing different throttle limit
        for (int throttle = 2; throttle < 1200; throttle += 21)
        {
            origin = partitions(metadata.regularAndStaticColumns(),
                                DeletionTime.build(0, 100),
                                createStaticRow(createCell(staticMetadata, 160)),
                                partitions);
            throttledIterator = ThrottledUnfilteredIterator.throttle(origin, throttle);

            int currentPartition = 0;
            int rowsInPartition = 0;
            int expectedSplitCount = 0;
            int currentSplit = 1;
            while (throttledIterator.hasNext())
            {
                UnfilteredRowIterator splitted = throttledIterator.next();
                if (currentSplit > expectedSplitCount)
                {
                    currentPartition++;
                    rowsInPartition = partitions.get(currentPartition).size();
                    expectedSplitCount = (int) Math.ceil(rowsInPartition * 1.0 / throttle);
                    currentSplit = 1;
                }
                UnfilteredRowIterator current = rows(metadata.regularAndStaticColumns(),
                                                     currentPartition,
                                                     DeletionTime.build(0, 100),
                                                     createStaticRow(createCell(staticMetadata, 160)),
                                                     partitions.get(currentPartition).toArray(new Row[0]));
                assertMetadata(current, splitted, currentSplit == 1);
                // no op
                splitted.close();

                int start = (currentSplit - 1) * throttle;
                int end = currentSplit == expectedSplitCount ? rowsInPartition : currentSplit * throttle;
                assertRows(splitted, partitions.get(currentPartition).subList(start, end).toArray(new Row[0]));
                currentSplit++;
            }
        }


        origin = partitions(metadata.regularAndStaticColumns(),
                            DeletionTime.build(0, 100),
                            Rows.EMPTY_STATIC_ROW,
                            partitions);
        try
        {
            try (CloseableIterator<UnfilteredRowIterator> throttled = ThrottledUnfilteredIterator.throttle(origin, 10))
            {
                int i = 0;
                while (throttled.hasNext())
                {
                    assertEquals(dk(1), throttled.next().partitionKey());
                    if (i++ == 10)
                    {
                        throw new RuntimeException("Dummy exception");
                    }
                }
                fail("Should not reach here");
            }
        }
        catch (RuntimeException rte)
        {
            int iteratedPartitions = 2;
            while (iteratedPartitions <= partitionCount)
            {
                // check that original iterator was not closed
                assertTrue(origin.hasNext());
                // check it's possible to fetch second partition from original iterator
                assertEquals(dk(iteratedPartitions++), origin.next().partitionKey());
            }
        }

    }

    private void assertMetadata(UnfilteredRowIterator origin, UnfilteredRowIterator splitted, boolean isFirst)
    {
        assertEquals(splitted.columns(), origin.columns());
        assertEquals(splitted.partitionKey(), origin.partitionKey());
        assertEquals(splitted.isReverseOrder(), origin.isReverseOrder());
        assertEquals(splitted.metadata(), origin.metadata());
        assertEquals(splitted.stats(), origin.stats());

        if (isFirst)
        {
            assertEquals(origin.partitionLevelDeletion(), splitted.partitionLevelDeletion());
            assertEquals(origin.staticRow(), splitted.staticRow());
        }
        else
        {
            assertEquals(DeletionTime.LIVE, splitted.partitionLevelDeletion());
            assertEquals(Rows.EMPTY_STATIC_ROW, splitted.staticRow());
        }
    }

    public static void assertRows(UnfilteredRowIterator iterator, Row... rows)
    {
        Iterator<Row> rowsIterator = Arrays.asList(rows).iterator();

        while (iterator.hasNext() && rowsIterator.hasNext())
            assertEquals(iterator.next(), rowsIterator.next());

        assertTrue(iterator.hasNext() == rowsIterator.hasNext());
    }

    private static DecoratedKey dk(int pk)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(pk), ByteBufferUtil.bytes(pk));
    }

    private static UnfilteredRowIterator rows(RegularAndStaticColumns columns,
                                              int pk,
                                              DeletionTime partitionDeletion,
                                              Row staticRow,
                                              Unfiltered... rows)
    {
        Iterator<Unfiltered> rowsIterator = Arrays.asList(rows).iterator();
        return new AbstractUnfilteredRowIterator(metadata, dk(pk), partitionDeletion, columns, staticRow, false, EncodingStats.NO_STATS) {
            protected Unfiltered computeNext()
            {
                return rowsIterator.hasNext() ? rowsIterator.next() : endOfData();
            }
        };
    }

    private static UnfilteredPartitionIterator partitions(RegularAndStaticColumns columns,
                                                          DeletionTime partitionDeletion,
                                                          Row staticRow,
                                                          SortedMap<Integer, List<Row>> partitions)
    {
        Iterator<Map.Entry<Integer, List<Row>>> partitionIt = partitions.entrySet().iterator();
        return new AbstractUnfilteredPartitionIterator() {
            public boolean hasNext()
            {
                return partitionIt.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                Map.Entry<Integer, List<Row>> next = partitionIt.next();
                Iterator<Row> rowsIterator = next.getValue().iterator();
                return new AbstractUnfilteredRowIterator(metadata, dk(next.getKey()), partitionDeletion, columns, staticRow, false, EncodingStats.NO_STATS) {
                    protected Unfiltered computeNext()
                    {
                        return rowsIterator.hasNext() ? rowsIterator.next() : endOfData();
                    }
                };
            }

            public TableMetadata metadata()
            {
                return metadata;
            }
        };
    }


    private static Row createRow(int ck, Cell<?>... columns)
    {
        return createRow(ck, ck, columns);
    }

    private static Row createRow(int ck1, int ck2, Cell<?>... columns)
    {
        BTreeRow.Builder builder = new BTreeRow.Builder(true);
        builder.newRow(Util.clustering(metadata.comparator, ck1, ck2));
        for (Cell<?> cell : columns)
            builder.addCell(cell);
        return builder.build();
    }

    private static Row createStaticRow(Cell<?>... columns)
    {
        Row.Builder builder = new BTreeRow.Builder(true);
        builder.newRow(Clustering.STATIC_CLUSTERING);
        for (Cell<?> cell : columns)
            builder.addCell(cell);
        return builder.build();
    }

    private static Cell<?> createCell(ColumnMetadata metadata, int v)
    {
        return createCell(metadata, v, 100L, BufferCell.NO_DELETION_TIME);
    }

    private static Cell<?> createCell(ColumnMetadata metadata, int v, long timestamp, long localDeletionTime)
    {
        return new BufferCell(metadata,
                              timestamp,
                              BufferCell.NO_TTL,
                              localDeletionTime,
                              ByteBufferUtil.bytes(v),
                              null);
    }

    @Test
    public void testThrottledIteratorWithRangeDeletions() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k1";

        UpdateBuilder builder;

        builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 0; i < 40; i += 2)
            builder.newRow(i).add("val", i);
        builder.applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 1, key).addRangeTombstone(10, 22).build().applyUnsafe();

        Util.flush(cfs);

        builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(2);
        for (int i = 1; i < 40; i += 2)
            builder.newRow(i).add("val", i);
        builder.applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 3, key).addRangeTombstone(19, 27).build().applyUnsafe();
        // We don't flush to test with both a range tomsbtone in memtable and in sstable

        // Queries by name
        int[] live = new int[]{ 4, 9, 11, 17, 28 };
        int[] dead = new int[]{ 12, 19, 21, 24, 27 };

        AbstractReadCommandBuilder.PartitionRangeBuilder cmdBuilder = Util.cmd(cfs);

        ReadCommand cmd = cmdBuilder.build();

        for (int batchSize = 2; batchSize <= 40; batchSize++)
        {
            List<UnfilteredRowIterator> unfilteredRowIterators = new LinkedList<>();

            try (ReadExecutionController executionController = cmd.executionController();
                 UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
            {
                assertTrue(iterator.hasNext());
                Iterator<UnfilteredRowIterator> throttled = ThrottledUnfilteredIterator.throttle(iterator, batchSize);
                while (throttled.hasNext())
                {
                    UnfilteredRowIterator next = throttled.next();
                    ImmutableBTreePartition materializedPartition = ImmutableBTreePartition.create(next);
                    int unfilteredCount = Iterators.size(materializedPartition.unfilteredIterator());

                    System.out.println("batchsize " + batchSize + " unfilteredCount " + unfilteredCount + " materializedPartition " + materializedPartition);

                    if (throttled.hasNext())
                    {
                        if (unfilteredCount != batchSize)
                        {
                            //when there is extra unfiltered, it must be close bound marker
                            assertEquals(batchSize + 1, unfilteredCount);
                            Unfiltered last = Iterators.getLast(materializedPartition.unfilteredIterator());
                            assertTrue(last.isRangeTombstoneMarker());
                            RangeTombstoneMarker marker = (RangeTombstoneMarker) last;
                            assertFalse(marker.isBoundary());
                            assertTrue(marker.isClose(false));
                        }
                    }
                    else
                    {
                        //only last batch can be smaller than batchSize
                        assertTrue(unfilteredCount <= batchSize + 1);
                    }
                    unfilteredRowIterators.add(materializedPartition.unfilteredIterator());
                }
                assertFalse(iterator.hasNext());
            }

            // Verify throttled data after merge
            Partition partition = ImmutableBTreePartition.create(UnfilteredRowIterators.merge(unfilteredRowIterators));

            long nowInSec = FBUtilities.nowInSeconds();

            for (int i : live)
                assertTrue("Row " + i + " should be live", partition.getRow(Clustering.make(ByteBufferUtil.bytes((i)))).hasLiveData(nowInSec, cfs.metadata().enforceStrictLiveness()));
            for (int i : dead)
                assertFalse("Row " + i + " shouldn't be live", partition.getRow(Clustering.make(ByteBufferUtil.bytes((i)))).hasLiveData(nowInSec, cfs.metadata().enforceStrictLiveness()));
        }
    }
}
