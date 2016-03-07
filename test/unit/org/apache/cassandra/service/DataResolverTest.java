/*
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
 */
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.assertClustering;
import static org.apache.cassandra.Util.assertColumn;
import static org.apache.cassandra.Util.assertColumns;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataResolverTest
{
    public static final String KEYSPACE1 = "DataResolverTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_COLLECTION = "Collection1";

    // counter to generate the last byte of the respondent's address in a ReadResponse message
    private int addressSuffix = 10;

    private DecoratedKey dk;
    private Keyspace ks;
    private ColumnFamilyStore cfs;
    private ColumnFamilyStore cfs2;
    private CFMetaData cfm;
    private CFMetaData cfm2;
    private ColumnDefinition m;
    private int nowInSec;
    private ReadCommand command;
    private MessageRecorder messageRecorder;


    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CFMetaData cfMetadata = CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD)
                                                  .addPartitionKey("key", BytesType.instance)
                                                  .addClusteringColumn("col1", AsciiType.instance)
                                                  .addRegularColumn("c1", AsciiType.instance)
                                                  .addRegularColumn("c2", AsciiType.instance)
                                                  .addRegularColumn("one", AsciiType.instance)
                                                  .addRegularColumn("two", AsciiType.instance)
                                                  .build();

        CFMetaData cfMetaData2 = CFMetaData.Builder.create(KEYSPACE1, CF_COLLECTION)
                                                   .addPartitionKey("k", ByteType.instance)
                                                   .addRegularColumn("m", MapType.getInstance(IntegerType.instance, IntegerType.instance, true))
                                                   .build();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    cfMetadata, cfMetaData2);
    }

    @Before
    public void setup()
    {
        dk = Util.dk("key1");
        ks = Keyspace.open(KEYSPACE1);
        cfs = ks.getColumnFamilyStore(CF_STANDARD);
        cfm = cfs.metadata;
        cfs2 = ks.getColumnFamilyStore(CF_COLLECTION);
        cfm2 = cfs2.metadata;
        m = cfm2.getColumnDefinition(new ColumnIdentifier("m", false));

        nowInSec = FBUtilities.nowInSeconds();
        command = Util.cmd(cfs, dk).withNowInSeconds(nowInSec).build();
    }

    @Before
    public void injectMessageSink()
    {
        // install an IMessageSink to capture all messages
        // so we can inspect them during tests
        messageRecorder = new MessageRecorder();
        MessagingService.instance().addMessageSink(messageRecorder);
    }

    @After
    public void removeMessageSink()
    {
        // should be unnecessary, but good housekeeping
        MessagingService.instance().clearMessageSinks();
    }

    @Test
    public void testResolveNewerSingleRow() throws UnknownHostException
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                       .add("c1", "v1")
                                                                                                       .buildUpdate())));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                       .add("c1", "v2")
                                                                                                       .buildUpdate())));

        try(PartitionIterator data = resolver.resolve();
            RowIterator rows = Iterators.getOnlyElement(data))
        {
            Row row = Iterators.getOnlyElement(rows);
            assertColumns(row, "c1");
            assertColumn(cfm, row, "c1", "v2", 1);
        }

        assertEquals(1, messageRecorder.sent.size());
        // peer 1 just needs to repair with the row from peer 2
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "1", "c1", "v2", 1);
    }

    @Test
    public void testResolveDisjointSingleRow()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                       .add("c1", "v1")
                                                                                                       .buildUpdate())));

        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                       .add("c2", "v2")
                                                                                                       .buildUpdate())));

        try(PartitionIterator data = resolver.resolve();
            RowIterator rows = Iterators.getOnlyElement(data))
        {
            Row row = Iterators.getOnlyElement(rows);
            assertColumns(row, "c1", "c2");
            assertColumn(cfm, row, "c1", "v1", 0);
            assertColumn(cfm, row, "c2", "v2", 1);
        }

        assertEquals(2, messageRecorder.sent.size());
        // each peer needs to repair with each other's column
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsColumn(msg, "1", "c2", "v2", 1);

        msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsColumn(msg, "1", "c1", "v1", 0);
    }

    @Test
    public void testResolveDisjointMultipleRows() throws UnknownHostException
    {

        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                       .add("c1", "v1")
                                                                                                       .buildUpdate())));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("2")
                                                                                                       .add("c2", "v2")
                                                                                                       .buildUpdate())));

        try (PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = data.next())
            {
                // We expect the resolved superset to contain both rows
                Row row = rows.next();
                assertClustering(cfm, row, "1");
                assertColumns(row, "c1");
                assertColumn(cfm, row, "c1", "v1", 0);

                row = rows.next();
                assertClustering(cfm, row, "2");
                assertColumns(row, "c2");
                assertColumn(cfm, row, "c2", "v2", 1);

                assertFalse(rows.hasNext());
                assertFalse(data.hasNext());
            }
        }

        assertEquals(2, messageRecorder.sent.size());
        // each peer needs to repair the row from the other
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "2", "c2", "v2", 1);

        msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "1", "c1", "v1", 0);
    }

    @Test
    public void testResolveDisjointMultipleRowsWithRangeTombstones()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 4);

        RangeTombstone tombstone1 = tombstone("1", "11", 1, nowInSec);
        RangeTombstone tombstone2 = tombstone("3", "31", 1, nowInSec);
        PartitionUpdate update =new RowUpdateBuilder(cfm, nowInSec, 1L, dk).addRangeTombstone(tombstone1)
                                                                                  .addRangeTombstone(tombstone2)
                                                                                  .buildUpdate();

        InetAddress peer1 = peer();
        UnfilteredPartitionIterator iter1 = iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).addRangeTombstone(tombstone1)
                                                                                  .addRangeTombstone(tombstone2)
                                                                                  .buildUpdate());
        resolver.preprocess(readResponseMessage(peer1, iter1));
        // not covered by any range tombstone
        InetAddress peer2 = peer();
        UnfilteredPartitionIterator iter2 = iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("0")
                                                                                  .add("c1", "v0")
                                                                                  .buildUpdate());
        resolver.preprocess(readResponseMessage(peer2, iter2));
        // covered by a range tombstone
        InetAddress peer3 = peer();
        UnfilteredPartitionIterator iter3 = iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("10")
                                                                                  .add("c2", "v1")
                                                                                  .buildUpdate());
        resolver.preprocess(readResponseMessage(peer3, iter3));
        // range covered by rt, but newer
        InetAddress peer4 = peer();
        UnfilteredPartitionIterator iter4 = iter(new RowUpdateBuilder(cfm, nowInSec, 2L, dk).clustering("3")
                                                                                  .add("one", "A")
                                                                                  .buildUpdate());
        resolver.preprocess(readResponseMessage(peer4, iter4));
        try (PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = data.next())
            {
                Row row = rows.next();
                assertClustering(cfm, row, "0");
                assertColumns(row, "c1");
                assertColumn(cfm, row, "c1", "v0", 0);

                row = rows.next();
                assertClustering(cfm, row, "3");
                assertColumns(row, "one");
                assertColumn(cfm, row, "one", "A", 2);

                assertFalse(rows.hasNext());
            }
        }

        assertEquals(4, messageRecorder.sent.size());
        // peer1 needs the rows from peers 2 and 4
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "0", "c1", "v0", 0);
        assertRepairContainsColumn(msg, "3", "one", "A", 2);

        // peer2 needs to get the row from peer4 and the RTs
        msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, null, tombstone1, tombstone2);
        assertRepairContainsColumn(msg, "3", "one", "A", 2);

        // peer 3 needs both rows and the RTs
        msg = getSentMessage(peer3);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, null, tombstone1, tombstone2);
        assertRepairContainsColumn(msg, "0", "c1", "v0", 0);
        assertRepairContainsColumn(msg, "3", "one", "A", 2);

        // peer4 needs the row from peer2  and the RTs
        msg = getSentMessage(peer4);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, null, tombstone1, tombstone2);
        assertRepairContainsColumn(msg, "0", "c1", "v0", 0);
    }

    @Test
    public void testResolveWithOneEmpty()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                       .add("c2", "v2")
                                                                                                       .buildUpdate())));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, EmptyIterators.unfilteredPartition(cfm, false)));

        try(PartitionIterator data = resolver.resolve();
            RowIterator rows = Iterators.getOnlyElement(data))
        {
            Row row = Iterators.getOnlyElement(rows);
            assertColumns(row, "c2");
            assertColumn(cfm, row, "c2", "v2", 1);
        }

        assertEquals(1, messageRecorder.sent.size());
        // peer 2 needs the row from peer 1
        MessageOut msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "1", "c2", "v2", 1);
    }

    @Test
    public void testResolveWithBothEmpty()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        resolver.preprocess(readResponseMessage(peer(), EmptyIterators.unfilteredPartition(cfm, false)));
        resolver.preprocess(readResponseMessage(peer(), EmptyIterators.unfilteredPartition(cfm, false)));

        try(PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        assertTrue(messageRecorder.sent.isEmpty());
    }

    @Test
    public void testResolveDeleted()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2);
        // one response with columns timestamped before a delete in another response
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                       .add("one", "A")
                                                                                                       .buildUpdate())));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, fullPartitionDelete(cfm, dk, 1, nowInSec)));

        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        // peer1 should get the deletion from peer2
        assertEquals(1, messageRecorder.sent.size());
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, new DeletionTime(1, nowInSec));
        assertRepairContainsNoColumns(msg);
    }

    @Test
    public void testResolveMultipleDeleted()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 4);
        // deletes and columns with interleaved timestamp, with out of order return sequence
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, fullPartitionDelete(cfm, dk, 0, nowInSec)));
        // these columns created after the previous deletion
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                       .add("one", "A")
                                                                                                       .add("two", "A")
                                                                                                       .buildUpdate())));
        //this column created after the next delete
        InetAddress peer3 = peer();
        resolver.preprocess(readResponseMessage(peer3, iter(new RowUpdateBuilder(cfm, nowInSec, 3L, dk).clustering("1")
                                                                                                       .add("two", "B")
                                                                                                       .buildUpdate())));
        InetAddress peer4 = peer();
        resolver.preprocess(readResponseMessage(peer4, fullPartitionDelete(cfm, dk, 2, nowInSec)));

        try(PartitionIterator data = resolver.resolve();
            RowIterator rows = Iterators.getOnlyElement(data))
        {
            Row row = Iterators.getOnlyElement(rows);
            assertColumns(row, "two");
            assertColumn(cfm, row, "two", "B", 3);
        }

        // peer 1 needs to get the partition delete from peer 4 and the row from peer 3
        assertEquals(4, messageRecorder.sent.size());
        MessageOut msg = getSentMessage(peer1);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, new DeletionTime(2, nowInSec));
        assertRepairContainsColumn(msg, "1", "two", "B", 3);

        // peer 2 needs the deletion from peer 4 and the row from peer 3
        msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, new DeletionTime(2, nowInSec));
        assertRepairContainsColumn(msg, "1", "two", "B", 3);

        // peer 3 needs just the deletion from peer 4
        msg = getSentMessage(peer3);
        assertRepairMetadata(msg);
        assertRepairContainsDeletions(msg, new DeletionTime(2, nowInSec));
        assertRepairContainsNoColumns(msg);

        // peer 4 needs just the row from peer 3
        msg = getSentMessage(peer4);
        assertRepairMetadata(msg);
        assertRepairContainsNoDeletions(msg);
        assertRepairContainsColumn(msg, "1", "two", "B", 3);
    }

    private static ByteBuffer bb(int b)
    {
        return ByteBufferUtil.bytes(b);
    }

    private Cell mapCell(int k, int v, long ts)
    {
        return BufferCell.live(m, ts, bb(v), CellPath.create(bb(k)));
    }

    @Test
    public void testResolveComplexDelete()
    {
        ReadCommand cmd = Util.cmd(cfs2, dk).withNowInSeconds(nowInSec).build();
        DataResolver resolver = new DataResolver(ks, cmd, ConsistencyLevel.ALL, 2);

        long[] ts = {100, 200};

        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSec);
        builder.newRow(Clustering.EMPTY);
        builder.addComplexDeletion(m, new DeletionTime(ts[0] - 1, nowInSec));
        builder.addCell(mapCell(0, 0, ts[0]));

        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build())), cmd));

        builder.newRow(Clustering.EMPTY);
        DeletionTime expectedCmplxDelete = new DeletionTime(ts[1] - 1, nowInSec);
        builder.addComplexDeletion(m, expectedCmplxDelete);
        Cell expectedCell = mapCell(1, 1, ts[1]);
        builder.addCell(expectedCell);

        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build())), cmd));

        try(PartitionIterator data = resolver.resolve(); RowIterator rows = Iterators.getOnlyElement(data))
        {
            Row row = Iterators.getOnlyElement(rows);
            assertColumns(row, "m");
            Assert.assertNull(row.getCell(m, CellPath.create(bb(0))));
            Assert.assertNotNull(row.getCell(m, CellPath.create(bb(1))));
        }

        MessageOut<Mutation> msg;
        msg = getSentMessage(peer1);
        Iterator<Row> rowIter = msg.payload.getPartitionUpdate(cfm2.cfId).iterator();
        assertTrue(rowIter.hasNext());
        Row row = rowIter.next();
        assertFalse(rowIter.hasNext());

        ComplexColumnData cd = row.getComplexColumnData(m);

        assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
        assertEquals(expectedCmplxDelete, cd.complexDeletion());

        Assert.assertNull(messageRecorder.sent.get(peer2));
    }

    @Test
    public void testResolveDeletedCollection()
    {

        ReadCommand cmd = Util.cmd(cfs2, dk).withNowInSeconds(nowInSec).build();
        DataResolver resolver = new DataResolver(ks, cmd, ConsistencyLevel.ALL, 2);

        long[] ts = {100, 200};

        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSec);
        builder.newRow(Clustering.EMPTY);
        builder.addComplexDeletion(m, new DeletionTime(ts[0] - 1, nowInSec));
        builder.addCell(mapCell(0, 0, ts[0]));

        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build())), cmd));

        builder.newRow(Clustering.EMPTY);
        DeletionTime expectedCmplxDelete = new DeletionTime(ts[1] - 1, nowInSec);
        builder.addComplexDeletion(m, expectedCmplxDelete);

        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build())), cmd));

        try(PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        MessageOut<Mutation> msg;
        msg = getSentMessage(peer1);
        Iterator<Row> rowIter = msg.payload.getPartitionUpdate(cfm2.cfId).iterator();
        assertTrue(rowIter.hasNext());
        Row row = rowIter.next();
        assertFalse(rowIter.hasNext());

        ComplexColumnData cd = row.getComplexColumnData(m);

        assertEquals(Collections.emptySet(), Sets.newHashSet(cd));
        assertEquals(expectedCmplxDelete, cd.complexDeletion());

        Assert.assertNull(messageRecorder.sent.get(peer2));
    }

    @Test
    public void testResolveNewCollection()
    {
        ReadCommand cmd = Util.cmd(cfs2, dk).withNowInSeconds(nowInSec).build();
        DataResolver resolver = new DataResolver(ks, cmd, ConsistencyLevel.ALL, 2);

        long[] ts = {100, 200};

        // map column
        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSec);
        builder.newRow(Clustering.EMPTY);
        DeletionTime expectedCmplxDelete = new DeletionTime(ts[0] - 1, nowInSec);
        builder.addComplexDeletion(m, expectedCmplxDelete);
        Cell expectedCell = mapCell(0, 0, ts[0]);
        builder.addCell(expectedCell);

        // empty map column
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build())), cmd));

        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(PartitionUpdate.emptyUpdate(cfm2, dk))));

        try(PartitionIterator data = resolver.resolve(); RowIterator rows = Iterators.getOnlyElement(data))
        {
            Row row = Iterators.getOnlyElement(rows);
            assertColumns(row, "m");
            ComplexColumnData cd = row.getComplexColumnData(m);
            assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
        }

        Assert.assertNull(messageRecorder.sent.get(peer1));

        MessageOut<Mutation> msg;
        msg = getSentMessage(peer2);
        Iterator<Row> rowIter = msg.payload.getPartitionUpdate(cfm2.cfId).iterator();
        assertTrue(rowIter.hasNext());
        Row row = rowIter.next();
        assertFalse(rowIter.hasNext());

        ComplexColumnData cd = row.getComplexColumnData(m);

        assertEquals(Sets.newHashSet(expectedCell), Sets.newHashSet(cd));
        assertEquals(expectedCmplxDelete, cd.complexDeletion());
    }

    @Test
    public void testResolveNewCollectionOverwritingDeleted()
    {
        ReadCommand cmd = Util.cmd(cfs2, dk).withNowInSeconds(nowInSec).build();
        DataResolver resolver = new DataResolver(ks, cmd, ConsistencyLevel.ALL, 2);

        long[] ts = {100, 200};

        // cleared map column
        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSec);
        builder.newRow(Clustering.EMPTY);
        builder.addComplexDeletion(m, new DeletionTime(ts[0] - 1, nowInSec));

        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build())), cmd));

        // newer, overwritten map column
        builder.newRow(Clustering.EMPTY);
        DeletionTime expectedCmplxDelete = new DeletionTime(ts[1] - 1, nowInSec);
        builder.addComplexDeletion(m, expectedCmplxDelete);
        Cell expectedCell = mapCell(1, 1, ts[1]);
        builder.addCell(expectedCell);

        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build())), cmd));

        try(PartitionIterator data = resolver.resolve(); RowIterator rows = Iterators.getOnlyElement(data))
        {
            Row row = Iterators.getOnlyElement(rows);
            assertColumns(row, "m");
            ComplexColumnData cd = row.getComplexColumnData(m);
            assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
        }

        MessageOut<Mutation> msg;
        msg = getSentMessage(peer1);
        Row row = Iterators.getOnlyElement(msg.payload.getPartitionUpdate(cfm2.cfId).iterator());

        ComplexColumnData cd = row.getComplexColumnData(m);

        assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
        assertEquals(expectedCmplxDelete, cd.complexDeletion());

        Assert.assertNull(messageRecorder.sent.get(peer2));
    }

    private InetAddress peer()
    {
        try
        {
            return InetAddress.getByAddress(new byte[]{ 127, 0, 0, (byte) addressSuffix++ });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private MessageOut<Mutation> getSentMessage(InetAddress target)
    {
        MessageOut<Mutation> message = messageRecorder.sent.get(target);
        assertNotNull(String.format("No repair message was sent to %s", target), message);
        return message;
    }

    private void assertRepairContainsDeletions(MessageOut<Mutation> message,
                                               DeletionTime deletionTime,
                                               RangeTombstone...rangeTombstones)
    {
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        DeletionInfo deletionInfo = update.deletionInfo();
        if (deletionTime != null)
            assertEquals(deletionTime, deletionInfo.getPartitionDeletion());

        assertEquals(rangeTombstones.length, deletionInfo.rangeCount());
        Iterator<RangeTombstone> ranges = deletionInfo.rangeIterator(false);
        int i = 0;
        while (ranges.hasNext())
        {
            assertEquals(ranges.next(), rangeTombstones[i++]);
        }
    }

    private void assertRepairContainsNoDeletions(MessageOut<Mutation> message)
    {
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        assertTrue(update.deletionInfo().isLive());
    }

    private void assertRepairContainsColumn(MessageOut<Mutation> message,
                                            String clustering,
                                            String columnName,
                                            String value,
                                            long timestamp)
    {
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        Row row = update.getRow(update.metadata().comparator.make(clustering));
        assertNotNull(row);
        assertColumn(cfm, row, columnName, value, timestamp);
    }

    private void assertRepairContainsNoColumns(MessageOut<Mutation> message)
    {
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        assertFalse(update.iterator().hasNext());
    }

    private void assertRepairMetadata(MessageOut<Mutation> message)
    {
        assertEquals(MessagingService.Verb.READ_REPAIR, message.verb);
        PartitionUpdate update = ((Mutation)message.payload).getPartitionUpdates().iterator().next();
        assertEquals(update.metadata().ksName, cfm.ksName);
        assertEquals(update.metadata().cfName, cfm.cfName);
    }


    public MessageIn<ReadResponse> readResponseMessage(InetAddress from, UnfilteredPartitionIterator partitionIterator)
    {
        return readResponseMessage(from, partitionIterator, command);

    }
    public MessageIn<ReadResponse> readResponseMessage(InetAddress from, UnfilteredPartitionIterator partitionIterator, ReadCommand cmd)
    {
        return MessageIn.create(from,
                                ReadResponse.createRemoteDataResponse(partitionIterator, cmd),
                                Collections.EMPTY_MAP,
                                MessagingService.Verb.REQUEST_RESPONSE,
                                MessagingService.current_version,
                                MessageIn.createTimestamp());
    }

    private RangeTombstone tombstone(Object start, Object end, long markedForDeleteAt, int localDeletionTime)
    {
        return new RangeTombstone(Slice.make(cfm.comparator.make(start), cfm.comparator.make(end)),
                                  new DeletionTime(markedForDeleteAt, localDeletionTime));
    }

    private UnfilteredPartitionIterator fullPartitionDelete(CFMetaData cfm, DecoratedKey dk, long timestamp, int nowInSec)
    {
        return new SingletonUnfilteredPartitionIterator(PartitionUpdate.fullPartitionDelete(cfm, dk, timestamp, nowInSec).unfilteredIterator(), false);
    }

    private static class MessageRecorder implements IMessageSink
    {
        Map<InetAddress, MessageOut> sent = new HashMap<>();
        public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
        {
            sent.put(to, message);
            return false;
        }

        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            return false;
        }
    }

    private UnfilteredPartitionIterator iter(PartitionUpdate update)
    {
        return new SingletonUnfilteredPartitionIterator(update.unfilteredIterator(), false);
    }
}
