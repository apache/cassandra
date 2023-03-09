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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.filter.AbstractClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SinglePartitionSliceCommandTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    private static TableMetadata metadata;
    private static ColumnMetadata v;
    private static ColumnMetadata s;

    private static final String TABLE_SCLICES = "tbl_slices";
    private static TableMetadata CFM_SLICES;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        metadata =
            TableMetadata.builder(KEYSPACE, TABLE)
                         .addPartitionKeyColumn("k", UTF8Type.instance)
                         .addStaticColumn("s", UTF8Type.instance)
                         .addClusteringColumn("i", IntegerType.instance)
                         .addRegularColumn("v", UTF8Type.instance)
                         .build();

        CFM_SLICES = TableMetadata.builder(KEYSPACE, TABLE_SCLICES)
                                  .addPartitionKeyColumn("k", UTF8Type.instance)
                                  .addClusteringColumn("c1", Int32Type.instance)
                                  .addClusteringColumn("c2", Int32Type.instance)
                                  .addRegularColumn("v", IntegerType.instance)
                                  .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), metadata, CFM_SLICES);
        v = metadata.getColumn(new ColumnIdentifier("v", true));
        s = metadata.getColumn(new ColumnIdentifier("s", true));
    }

    @Before
    public void truncate()
    {
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_SCLICES).truncateBlocking();
    }

    @Test
    public void testMultiNamesCommandWithFlush()
    {
        testMultiNamesOrSlicesCommand(true, false);
    }

    @Test
    public void testMultiNamesCommandWithoutFlush()
    {
        testMultiNamesOrSlicesCommand(false, false);
    }

    @Test
    public void testMultiSlicesCommandWithFlush()
    {
        testMultiNamesOrSlicesCommand(true, true);
    }

    @Test
    public void testMultiSlicesCommandWithoutFlush()
    {
        testMultiNamesOrSlicesCommand(false, true);
    }

    private AbstractClusteringIndexFilter createClusteringFilter(int uniqueCk1, int uniqueCk2, boolean isSlice)
    {
        Slices.Builder slicesBuilder = new Slices.Builder(CFM_SLICES.comparator);
        BTreeSet.Builder<Clustering<?>> namesBuilder = BTreeSet.builder(CFM_SLICES.comparator);

        for (int ck1 = 0; ck1 < uniqueCk1; ck1++)
        {
            for (int ck2 = 0; ck2 < uniqueCk2; ck2++)
            {
                if (isSlice)
                    slicesBuilder.add(Slice.make(Util.clustering(CFM_SLICES.comparator, ck1, ck2)));
                else
                    namesBuilder.add(Util.clustering(CFM_SLICES.comparator, ck1, ck2));
            }
        }
        if (isSlice)
            return new ClusteringIndexSliceFilter(slicesBuilder.build(), false);
        return new ClusteringIndexNamesFilter(namesBuilder.build(), false);
    }

    private void testMultiNamesOrSlicesCommand(boolean flush, boolean isSlice)
    {
        int deletionTime = 5;
        int ck1 = 1;
        int uniqueCk1 = 2;
        int uniqueCk2 = 3;

        DecoratedKey key = Util.dk(ByteBufferUtil.bytes("k"));
        QueryProcessor.executeInternal(String.format("DELETE FROM ks.tbl_slices USING TIMESTAMP %d WHERE k='k' AND c1=%d",
                                                     deletionTime,
                                                     ck1));

        if (flush)
            Util.flushTable(KEYSPACE, TABLE_SCLICES);

        AbstractClusteringIndexFilter clusteringFilter = createClusteringFilter(uniqueCk1, uniqueCk2, isSlice);
        ReadCommand cmd = SinglePartitionReadCommand.create(CFM_SLICES,
                                                            FBUtilities.nowInSeconds(),
                                                            ColumnFilter.all(CFM_SLICES),
                                                            RowFilter.none(),
                                                            DataLimits.NONE,
                                                            key,
                                                            clusteringFilter);

        UnfilteredPartitionIterator partitionIterator = cmd.executeLocally(cmd.executionController());
        assert partitionIterator.hasNext();
        UnfilteredRowIterator partition = partitionIterator.next();

        int count = 0;
        boolean open = true;
        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();

            assertTrue(unfiltered.isRangeTombstoneMarker());
            RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;

            // check if it's open-close pair
            assertEquals(open, marker.isOpen(false));
            // check deletion time same as Range Deletion
            DeletionTime delete = (open ? marker.openDeletionTime(false) : marker.closeDeletionTime(false));;
            assertEquals(deletionTime, delete.markedForDeleteAt());

            // check clustering values
            Clustering<?> clustering = Util.clustering(CFM_SLICES.comparator, ck1, count / 2);
            assertArrayEquals(clustering.getRawValues(), marker.clustering().getBufferArray());

            open = !open;
            count++;
        }
        assertEquals(uniqueCk2 * 2, count); // open and close range tombstones
    }

    private void checkForS(UnfilteredPartitionIterator pi)
    {
        Assert.assertTrue(pi.toString(), pi.hasNext());
        UnfilteredRowIterator ri = pi.next();
        Assert.assertTrue(ri.columns().contains(s));
        Row staticRow = ri.staticRow();
        Iterator<Cell<?>> cellIterator = staticRow.cells().iterator();
        Assert.assertTrue(staticRow.toString(metadata, true), cellIterator.hasNext());
        Cell<?> cell = cellIterator.next();
        Assert.assertEquals(s, cell.column());
        Assert.assertEquals(ByteBufferUtil.bytesToHex(cell.buffer()), ByteBufferUtil.bytes("s"), cell.buffer());
        Assert.assertFalse(cellIterator.hasNext());
    }

    @Test
    public void staticColumnsAreReturned() throws IOException
    {
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k1', 's')");
        Assert.assertFalse(QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k1'").isEmpty());

        ColumnFilter columnFilter = ColumnFilter.selection(RegularAndStaticColumns.of(s));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.NONE, false);
        ReadCommand cmd = SinglePartitionReadCommand.create(metadata,
                                                            FBUtilities.nowInSeconds(),
                                                            columnFilter,
                                                            RowFilter.none(),
                                                            DataLimits.NONE,
                                                            key,
                                                            sliceFilter);

        // check raw iterator for static cell
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            checkForS(pi);
        }

        ReadResponse response;
        DataOutputBuffer out;
        DataInputPlus in;
        ReadResponse dst;

        // check (de)serialized iterator for memtable static cell
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            response = ReadResponse.createDataResponse(pi, cmd, executionController.getRepairedDataInfo());
        }

        out = new DataOutputBuffer((int) ReadResponse.serializer.serializedSize(response, MessagingService.VERSION_40));
        ReadResponse.serializer.serialize(response, out, MessagingService.VERSION_40);
        in = new DataInputBuffer(out.buffer(), true);
        dst = ReadResponse.serializer.deserialize(in, MessagingService.VERSION_40);
        try (UnfilteredPartitionIterator pi = dst.makeIterator(cmd))
        {
            checkForS(pi);
        }

        // check (de)serialized iterator for sstable static cell
        Schema.instance.getColumnFamilyStoreInstance(metadata.id).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            response = ReadResponse.createDataResponse(pi, cmd, executionController.getRepairedDataInfo());
        }
        out = new DataOutputBuffer((int) ReadResponse.serializer.serializedSize(response, MessagingService.VERSION_40));
        ReadResponse.serializer.serialize(response, out, MessagingService.VERSION_40);
        in = new DataInputBuffer(out.buffer(), true);
        dst = ReadResponse.serializer.deserialize(in, MessagingService.VERSION_40);
        try (UnfilteredPartitionIterator pi = dst.makeIterator(cmd))
        {
            checkForS(pi);
        }
    }

    /**
     * Make sure point read on range tombstone returns the same physical data structure regardless
     * data is in memtable or sstable, so that we can produce the same digest.
     */
    @Test
    public void testReadOnRangeTombstoneMarker()
    {
        QueryProcessor.executeOnceInternal("CREATE TABLE IF NOT EXISTS ks.test_read_rt (k int, c1 int, c2 int, c3 int, v int, primary key (k, c1, c2, c3))");
        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "test_read_rt");
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);

        String template = "SELECT * FROM ks.test_read_rt %s";
        String pointRead = "WHERE k=1 and c1=1 and c2=1 and c3=1";
        String sliceReadC1C2 = "WHERE k=1 and c1=1 and c2=1";
        String sliceReadC1 = "WHERE k=1 and c1=1";
        String partitionRead = "WHERE k=1";

        for (String postfix : Arrays.asList(pointRead, sliceReadC1C2, sliceReadC1, partitionRead))
        {
            String query = String.format(template, postfix);
            cfs.truncateBlocking();
            QueryProcessor.executeOnceInternal("DELETE FROM ks.test_read_rt USING TIMESTAMP 10 WHERE k=1 AND c1=1");

            List<Unfiltered> memtableUnfiltereds = assertQueryReturnsSingleRT(query);
            Util.flush(cfs);
            List<Unfiltered> sstableUnfiltereds = assertQueryReturnsSingleRT(query);

            String errorMessage = String.format("Expected %s but got %s with postfix '%s'",
                                                toString(memtableUnfiltereds, metadata),
                                                toString(sstableUnfiltereds, metadata),
                                                postfix);
            assertEquals(errorMessage, memtableUnfiltereds, sstableUnfiltereds);
        }
    }

    /**
     * Partition deletion should remove row deletion when tie
     */
    @Test
    public void testPartitionDeletionRowDeletionTie()
    {
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.partition_row_deletion (k int, c int, v int, primary key (k, c))");
        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "partition_row_deletion");
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
        cfs.disableAutoCompaction();

        BiFunction<Boolean, Boolean, List<Unfiltered>> tester = (flush, multiSSTable)->
        {
            cfs.truncateBlocking();

            // timestamp and USING TIMESTAMP have different values to ensure the correct timestamp (the one specified in the
            // query) is the one being picked up. For safety reason we want to be able to ensure that further to its main goal
            // the test can also detect wrongful change of the code. The current timestamp retrieved from the ClientState is
            // ignored but nowInSeconds is retrieved from it and used for the DeletionTime.  It shows the difference between the
            // time at which the record was marked for deletion and the time at which it truly happened.
            final long timestamp = FBUtilities.timestampMicros();
            final long nowInSec = FBUtilities.nowInSeconds();

            QueryProcessor.executeOnceInternalWithNowAndTimestamp(nowInSec,
                                                                  timestamp,
                                                                  "DELETE FROM ks.partition_row_deletion USING TIMESTAMP 10 WHERE k=1");
            if (flush && multiSSTable)
                Util.flush(cfs);
            QueryProcessor.executeOnceInternalWithNowAndTimestamp(nowInSec,
                                                                  timestamp,
                                                                  "DELETE FROM ks.partition_row_deletion USING TIMESTAMP 10 WHERE k=1 and c=1");
            if (flush)
                Util.flush(cfs);

            QueryProcessor.executeOnceInternal("INSERT INTO ks.partition_row_deletion(k,c,v) VALUES(1,1,1) using timestamp 11");
            if (flush)
            {
                Util.flush(cfs);
                try
                {
                    cfs.forceMajorCompaction();
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            try (UnfilteredRowIterator partition = getIteratorFromSinglePartition("SELECT * FROM ks.partition_row_deletion where k=1 and c=1"))
            {
                assertEquals(10, partition.partitionLevelDeletion().markedForDeleteAt());
                return toUnfiltereds(partition);
            }
        };

        List<Unfiltered> memtableUnfiltereds = tester.apply(false, false);
        List<Unfiltered> singleSSTableUnfiltereds = tester.apply(true, false);
        List<Unfiltered> multiSSTableUnfiltereds = tester.apply(true, true);

        assertEquals(1, singleSSTableUnfiltereds.size());
        String errorMessage = String.format("Expected %s but got %s", toString(memtableUnfiltereds, metadata), toString(singleSSTableUnfiltereds, metadata));
        assertEquals(errorMessage, memtableUnfiltereds, singleSSTableUnfiltereds);
        errorMessage = String.format("Expected %s but got %s", toString(singleSSTableUnfiltereds, metadata), toString(multiSSTableUnfiltereds, metadata));
        assertEquals(errorMessage, singleSSTableUnfiltereds, multiSSTableUnfiltereds);
        memtableUnfiltereds.forEach(u -> assertTrue("Expected no row deletion, but got " + u.toString(metadata, true), ((Row) u).deletion().isLive()));
    }

    /**
     * Partition deletion should remove range deletion when tie
     */
    @Test
    public void testPartitionDeletionRangeDeletionTie()
    {
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.partition_range_deletion (k int, c1 int, c2 int, v int, primary key (k, c1, c2))");
        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "partition_range_deletion");
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
        cfs.disableAutoCompaction();

        BiFunction<Boolean, Boolean, List<Unfiltered>> tester = (flush, multiSSTable) ->
        {
            cfs.truncateBlocking();

            // timestamp and USING TIMESTAMP have different values to ensure the correct timestamp (the one specified in the
            // query) is the one being picked up. For safety reason we want to be able to ensure that further to its main goal
            // the test can also detect wrongful change of the code. The current timestamp retrieved from the ClientState is
            // ignored but nowInSeconds is retrieved from it and used for the DeletionTime.  It shows the difference between the
            // time at which the record was marked for deletion and the time at which it truly happened.

            final long timestamp = FBUtilities.timestampMicros();
            final long nowInSec = FBUtilities.nowInSeconds();

            QueryProcessor.executeOnceInternalWithNowAndTimestamp(nowInSec,
                                                                  timestamp,
                                                                  "DELETE FROM ks.partition_range_deletion USING TIMESTAMP 10 WHERE k=1");
            if (flush && multiSSTable)
                Util.flush(cfs);
            QueryProcessor.executeOnceInternalWithNowAndTimestamp(nowInSec,
                                                                  timestamp,
                                                                  "DELETE FROM ks.partition_range_deletion USING TIMESTAMP 10 WHERE k=1 and c1=1");
            if (flush)
                Util.flush(cfs);

            QueryProcessor.executeOnceInternal("INSERT INTO ks.partition_range_deletion(k,c1,c2,v) VALUES(1,1,1,1) using timestamp 11");
            if (flush)
            {
                Util.flush(cfs);
                try
                {
                    cfs.forceMajorCompaction();
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }

            try (UnfilteredRowIterator partition = getIteratorFromSinglePartition("SELECT * FROM ks.partition_range_deletion where k=1 and c1=1 and c2=1"))
            {
                assertEquals(10, partition.partitionLevelDeletion().markedForDeleteAt());
                return toUnfiltereds(partition);
            }
        };

        List<Unfiltered> memtableUnfiltereds = tester.apply(false, false);
        List<Unfiltered> singleSSTableUnfiltereds = tester.apply(true, false);
        List<Unfiltered> multiSSTableUnfiltereds = tester.apply(true, true);

        assertEquals(1, singleSSTableUnfiltereds.size());
        String errorMessage = String.format("Expected %s but got %s", toString(memtableUnfiltereds, metadata), toString(singleSSTableUnfiltereds, metadata));
        assertEquals(errorMessage, memtableUnfiltereds, singleSSTableUnfiltereds);
        errorMessage = String.format("Expected %s but got %s", toString(singleSSTableUnfiltereds, metadata), toString(multiSSTableUnfiltereds, metadata));
        assertEquals(errorMessage, singleSSTableUnfiltereds, multiSSTableUnfiltereds);
        memtableUnfiltereds.forEach(u -> assertTrue("Expected row, but got " + u.toString(metadata, true), u.isRow()));
    }

    @Test
    public void toCQLStringIsSafeToCall() throws IOException
    {
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));

        ColumnFilter columnFilter = ColumnFilter.selection(RegularAndStaticColumns.of(s));
        Slice slice = Slice.make(BufferClusteringBound.BOTTOM, BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes("i1")));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.with(metadata.comparator, slice), false);
        ReadCommand cmd = SinglePartitionReadCommand.create(metadata,
                                                            FBUtilities.nowInSeconds(),
                                                            columnFilter,
                                                            RowFilter.none(),
                                                            DataLimits.NONE,
                                                            key,
                                                            sliceFilter);
        String ret = cmd.toCQLString();
        Assert.assertNotNull(ret);
        Assert.assertFalse(ret.isEmpty());
    }

    public static UnfilteredRowIterator getIteratorFromSinglePartition(String q)
    {
        SelectStatement stmt = (SelectStatement) QueryProcessor.parseStatement(q).prepare(ClientState.forInternalCalls());

        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> query = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) stmt.getQuery(QueryOptions.DEFAULT, 0);
        Assert.assertEquals(1, query.queries.size());
        SinglePartitionReadCommand command = Iterables.getOnlyElement(query.queries);
        try (ReadExecutionController controller = ReadExecutionController.forCommand(command, false);
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            assert partitions.hasNext();
            UnfilteredRowIterator partition = partitions.next();
            assert !partitions.hasNext();
            return partition;
        }
    }

    public static List<Unfiltered> getUnfilteredsFromSinglePartition(String q)
    {
        try (UnfilteredRowIterator partition = getIteratorFromSinglePartition(q))
        {
            return toUnfiltereds(partition);
        }
    }

    private static List<Unfiltered> toUnfiltereds(UnfilteredRowIterator partition)
    {
        return Lists.newArrayList(partition);
    }

    private static List<Unfiltered> assertQueryReturnsSingleRT(String query)
    {
        List<Unfiltered> unfiltereds = getUnfilteredsFromSinglePartition(query);
        Assert.assertEquals(2, unfiltereds.size());
        Assert.assertTrue(unfiltereds.get(0).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(0)).isOpen(false));
        Assert.assertTrue(unfiltereds.get(1).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(1)).isClose(false));
        return unfiltereds;
    }

    private static ByteBuffer bb(int v)
    {
        return Int32Type.instance.decompose(v);
    }

    /**
     * tests the bug raised in CASSANDRA-14861, where the sstable min/max can
     * exclude range tombstones for clustering ranges not also covered by rows
     */
    @Test
    public void sstableFiltering()
    {
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.legacy_mc_inaccurate_min_max (k int, c1 int, c2 int, c3 int, v int, primary key (k, c1, c2, c3))");
        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "legacy_mc_inaccurate_min_max");
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);

        QueryProcessor.executeOnceInternal("INSERT INTO ks.legacy_mc_inaccurate_min_max (k, c1, c2, c3, v) VALUES (100, 2, 2, 2, 2)");
        QueryProcessor.executeOnceInternal("DELETE FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1");
        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1");
        Util.flush(cfs);
        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1");
        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1 AND c3=1"); // clustering names

        cfs.truncateBlocking();

        long nowMillis = System.currentTimeMillis();
        Slice slice = Slice.make(Clustering.make(bb(2), bb(3)), Clustering.make(bb(10), bb(10)));
        RangeTombstone rt = new RangeTombstone(slice, DeletionTime.build(TimeUnit.MILLISECONDS.toMicros(nowMillis),
                                                                       Ints.checkedCast(TimeUnit.MILLISECONDS.toSeconds(nowMillis))));

        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, bb(100), metadata.regularAndStaticColumns(), 1);
        builder.add(rt);
        new Mutation(builder.build()).apply();

        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=3 AND c2=2");
        Util.flush(cfs);
        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=3 AND c2=2");
        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=3 AND c2=2 AND c3=2"); // clustering names

    }

    private String toString(List<Unfiltered> unfiltereds, TableMetadata metadata)
    {
        return unfiltereds.stream().map(u -> u.toString(metadata, true)).collect(Collectors.toList()).toString();
    }
}
