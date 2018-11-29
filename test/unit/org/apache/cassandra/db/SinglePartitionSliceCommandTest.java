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

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
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
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTreeSet;

public class SinglePartitionSliceCommandTest
{
    private static final Logger logger = LoggerFactory.getLogger(SinglePartitionSliceCommandTest.class);

    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    private static CFMetaData cfm;
    private static ColumnDefinition v;
    private static ColumnDefinition s;

    private static final String TABLE_SCLICES = "tbl_slices";
    private static CFMetaData CFM_SLICES;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        cfm = CFMetaData.Builder.create(KEYSPACE, TABLE)
                                .addPartitionKey("k", UTF8Type.instance)
                                .addStaticColumn("s", UTF8Type.instance)
                                .addClusteringColumn("i", IntegerType.instance)
                                .addRegularColumn("v", UTF8Type.instance)
                                .build();

        CFM_SLICES = CFMetaData.Builder.create(KEYSPACE, TABLE_SCLICES)
                                       .addPartitionKey("k", UTF8Type.instance)
                                       .addClusteringColumn("c1", Int32Type.instance)
                                       .addClusteringColumn("c2", Int32Type.instance)
                                       .addRegularColumn("v", IntegerType.instance)
                                       .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm, CFM_SLICES);

        cfm = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
        v = cfm.getColumnDefinition(new ColumnIdentifier("v", true));
        s = cfm.getColumnDefinition(new ColumnIdentifier("s", true));

        CFM_SLICES = Schema.instance.getCFMetaData(KEYSPACE, TABLE_SCLICES);
    }

    @Before
    public void truncate()
    {
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_SCLICES).truncateBlocking();
    }

    @Test
    public void staticColumnsAreFiltered() throws IOException
    {
        DecoratedKey key = cfm.decorateKey(ByteBufferUtil.bytes("k"));

        UntypedResultSet rows;

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s, i, v) VALUES ('k', 's', 0, 'v')");
        QueryProcessor.executeInternal("DELETE v FROM ks.tbl WHERE k='k' AND i=0");
        QueryProcessor.executeInternal("DELETE FROM ks.tbl WHERE k='k' AND i=0");
        rows = QueryProcessor.executeInternal("SELECT * FROM ks.tbl WHERE k='k' AND i=0");

        for (UntypedResultSet.Row row: rows)
        {
            logger.debug("Current: k={}, s={}, v={}", (row.has("k") ? row.getString("k") : null), (row.has("s") ? row.getString("s") : null), (row.has("v") ? row.getString("v") : null));
        }

        assert rows.isEmpty();

        ColumnFilter columnFilter = ColumnFilter.selection(PartitionColumns.of(v));
        ByteBuffer zero = ByteBufferUtil.bytes(0);
        Slices slices = Slices.with(cfm.comparator, Slice.make(ClusteringBound.inclusiveStartOf(zero), ClusteringBound.inclusiveEndOf(zero)));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(slices, false);
        ReadCommand cmd = SinglePartitionReadCommand.create(true,
                                                            cfm,
                                                            FBUtilities.nowInSeconds(),
                                                            columnFilter,
                                                            RowFilter.NONE,
                                                            DataLimits.NONE,
                                                            key,
                                                            sliceFilter);

        DataOutputBuffer out = new DataOutputBuffer((int) ReadCommand.legacyReadCommandSerializer.serializedSize(cmd, MessagingService.VERSION_21));
        ReadCommand.legacyReadCommandSerializer.serialize(cmd, out, MessagingService.VERSION_21);
        DataInputPlus in = new DataInputBuffer(out.buffer(), true);
        cmd = ReadCommand.legacyReadCommandSerializer.deserialize(in, MessagingService.VERSION_21);

        logger.debug("ReadCommand: {}", cmd);
        try (ReadExecutionController controller = cmd.executionController();
             UnfilteredPartitionIterator partitionIterator = cmd.executeLocally(controller))
        {
            ReadResponse response = ReadResponse.createDataResponse(partitionIterator, cmd);

            logger.debug("creating response: {}", response);
            try (UnfilteredPartitionIterator pIter = response.makeIterator(cmd))
            {
                assert pIter.hasNext();
                try (UnfilteredRowIterator partition = pIter.next())
                {
                    LegacyLayout.LegacyUnfilteredPartition rowIter = LegacyLayout.fromUnfilteredRowIterator(cmd, partition);
                    Assert.assertEquals(Collections.emptyList(), rowIter.cells);
                }
            }
        }
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
        BTreeSet.Builder<Clustering> namesBuilder = BTreeSet.builder(CFM_SLICES.comparator);

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
        boolean isTombstone = flush || isSlice;
        int deletionTime = 5;
        int ck1 = 1;
        int uniqueCk1 = 2;
        int uniqueCk2 = 3;

        DecoratedKey key = CFM_SLICES.decorateKey(ByteBufferUtil.bytes("k"));
        QueryProcessor.executeInternal(String.format("DELETE FROM ks.tbl_slices USING TIMESTAMP %d WHERE k='k' AND c1=%d",
                                                     deletionTime,
                                                     ck1));

        if (flush)
            Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_SCLICES).forceBlockingFlush();

        AbstractClusteringIndexFilter clusteringFilter = createClusteringFilter(uniqueCk1, uniqueCk2, isSlice);
        ReadCommand cmd = SinglePartitionReadCommand.create(CFM_SLICES,
                                                            FBUtilities.nowInSeconds(),
                                                            ColumnFilter.all(CFM_SLICES),
                                                            RowFilter.NONE,
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
            if (isTombstone)
            {
                assertTrue(unfiltered.isRangeTombstoneMarker());
                RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;

                // check if it's open-close pair
                assertTrue(marker.isOpen(false) == open);
                // check deletion time same as Range Deletion
                if (open)
                    assertEquals(deletionTime, marker.openDeletionTime(false).markedForDeleteAt());
                else
                    assertEquals(deletionTime, marker.closeDeletionTime(false).markedForDeleteAt());

                // check clustering values
                Clustering clustering = Util.clustering(CFM_SLICES.comparator, ck1, count / 2);
                for (int i = 0; i < CFM_SLICES.comparator.size(); i++)
                {
                    int cmp = CFM_SLICES.comparator.compareComponent(i,
                                                                     clustering.getRawValues()[i],
                                                                     marker.clustering().values[i]);
                    assertEquals(0, cmp);
                }
                open = !open;
            }
            else
            {
                // deleted row
                assertTrue(unfiltered.isRow());
                Row row = (Row) unfiltered;
                assertEquals(deletionTime, row.deletion().time().markedForDeleteAt());
                assertEquals(0, row.columnCount()); // no btree
            }
            count++;
        }
        if (isTombstone)
            assertEquals(uniqueCk2 * 2, count); // open and close range tombstones
        else
            assertEquals(uniqueCk2, count);
    }

    private void checkForS(UnfilteredPartitionIterator pi)
    {
        Assert.assertTrue(pi.toString(), pi.hasNext());
        UnfilteredRowIterator ri = pi.next();
        Assert.assertTrue(ri.columns().contains(s));
        Row staticRow = ri.staticRow();
        Iterator<Cell> cellIterator = staticRow.cells().iterator();
        Assert.assertTrue(staticRow.toString(cfm, true), cellIterator.hasNext());
        Cell cell = cellIterator.next();
        Assert.assertEquals(s, cell.column());
        Assert.assertEquals(ByteBufferUtil.bytesToHex(cell.value()), ByteBufferUtil.bytes("s"), cell.value());
        Assert.assertFalse(cellIterator.hasNext());
    }

    @Test
    public void staticColumnsAreReturned() throws IOException
    {
        DecoratedKey key = cfm.decorateKey(ByteBufferUtil.bytes("k1"));

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k1', 's')");
        Assert.assertFalse(QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k1'").isEmpty());

        ColumnFilter columnFilter = ColumnFilter.selection(PartitionColumns.of(s));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.NONE, false);
        ReadCommand cmd = SinglePartitionReadCommand.create(true,
                                                            cfm,
                                                            FBUtilities.nowInSeconds(),
                                                            columnFilter,
                                                            RowFilter.NONE,
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
            response = ReadResponse.createDataResponse(pi, cmd);
        }

        out = new DataOutputBuffer((int) ReadResponse.serializer.serializedSize(response, MessagingService.VERSION_30));
        ReadResponse.serializer.serialize(response, out, MessagingService.VERSION_30);
        in = new DataInputBuffer(out.buffer(), true);
        dst = ReadResponse.serializer.deserialize(in, MessagingService.VERSION_30);
        try (UnfilteredPartitionIterator pi = dst.makeIterator(cmd))
        {
            checkForS(pi);
        }

        // check (de)serialized iterator for sstable static cell
        Schema.instance.getColumnFamilyStoreInstance(cfm.cfId).forceBlockingFlush();
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            response = ReadResponse.createDataResponse(pi, cmd);
        }
        out = new DataOutputBuffer((int) ReadResponse.serializer.serializedSize(response, MessagingService.VERSION_30));
        ReadResponse.serializer.serialize(response, out, MessagingService.VERSION_30);
        in = new DataInputBuffer(out.buffer(), true);
        dst = ReadResponse.serializer.deserialize(in, MessagingService.VERSION_30);
        try (UnfilteredPartitionIterator pi = dst.makeIterator(cmd))
        {
            checkForS(pi);
        }
    }

    @Test
    public void toCQLStringIsSafeToCall() throws IOException
    {
        DecoratedKey key = cfm.decorateKey(ByteBufferUtil.bytes("k1"));

        ColumnFilter columnFilter = ColumnFilter.selection(PartitionColumns.of(s));
        Slice slice = Slice.make(ClusteringBound.BOTTOM, ClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes("i1")));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.with(cfm.comparator, slice), false);
        ReadCommand cmd = SinglePartitionReadCommand.create(true,
                                                            cfm,
                                                            FBUtilities.nowInSeconds(),
                                                            columnFilter,
                                                            RowFilter.NONE,
                                                            DataLimits.NONE,
                                                            key,
                                                            sliceFilter);

        String ret = cmd.toCQLString();
        Assert.assertNotNull(ret);
        Assert.assertFalse(ret.isEmpty());
    }


    public static List<Unfiltered> getUnfilteredsFromSinglePartition(String q)
    {
        SelectStatement stmt = (SelectStatement) QueryProcessor.parseStatement(q).prepare(ClientState.forInternalCalls()).statement;

        List<Unfiltered> unfiltereds = new ArrayList<>();
        SinglePartitionReadCommand.Group query = (SinglePartitionReadCommand.Group) stmt.getQuery(QueryOptions.DEFAULT, 0);
        Assert.assertEquals(1, query.commands.size());
        SinglePartitionReadCommand command = Iterables.getOnlyElement(query.commands);
        try (ReadExecutionController controller = ReadExecutionController.forCommand(command);
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            assert partitions.hasNext();
            try (UnfilteredRowIterator partition = partitions.next())
            {
                while (partition.hasNext())
                {
                    Unfiltered next = partition.next();
                    unfiltereds.add(next);
                }
            }
            assert !partitions.hasNext();
        }
        return unfiltereds;
    }

    private static void assertQueryReturnsSingleRT(String query)
    {
        List<Unfiltered> unfiltereds = getUnfilteredsFromSinglePartition(query);
        Assert.assertEquals(2, unfiltereds.size());
        Assert.assertTrue(unfiltereds.get(0).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(0)).isOpen(false));
        Assert.assertTrue(unfiltereds.get(1).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(1)).isClose(false));
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
        CFMetaData metadata = Schema.instance.getCFMetaData("ks", "legacy_mc_inaccurate_min_max");
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.cfId);

        QueryProcessor.executeOnceInternal("INSERT INTO ks.legacy_mc_inaccurate_min_max (k, c1, c2, c3, v) VALUES (100, 2, 2, 2, 2)");
        QueryProcessor.executeOnceInternal("DELETE FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1");
        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1");
        cfs.forceBlockingFlush();
        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1");

        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1 AND c3=1"); // clustering names

        cfs.truncateBlocking();

        long nowMillis = System.currentTimeMillis();
        Slice slice = Slice.make(Clustering.make(bb(2), bb(3)), Clustering.make(bb(10), bb(10)));
        RangeTombstone rt = new RangeTombstone(slice, new DeletionTime(TimeUnit.MILLISECONDS.toMicros(nowMillis),
                                                                       Ints.checkedCast(TimeUnit.MILLISECONDS.toSeconds(nowMillis))));
        PartitionUpdate update = new PartitionUpdate(cfs.metadata, bb(100), cfs.metadata.partitionColumns(), 1);
        update.add(rt);
        new Mutation(update).apply();

        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=3 AND c2=2");
        cfs.forceBlockingFlush();
        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=3 AND c2=2");
        assertQueryReturnsSingleRT("SELECT * FROM ks.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=3 AND c2=2 AND c3=2"); // clustering names

    }
}
