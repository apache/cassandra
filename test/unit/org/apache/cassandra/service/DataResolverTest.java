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
import org.apache.cassandra.config.DatabaseDescriptor;
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
import static org.apache.cassandra.db.ClusteringBound.Kind;

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
        DatabaseDescriptor.daemonInitialization();
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

    /**
     * Checks that the provided data resolver has the expected number of repair futures created.
     * This method also "release" those future by faking replica responses to those repair, which is necessary or
     * every test would timeout when closing the result of resolver.resolve(), since it waits on those futures.
     */
    private void assertRepairFuture(DataResolver resolver, int expectedRepairs)
    {
        assertEquals(expectedRepairs, resolver.repairResults.size());

        // Signal all future. We pass a completely fake response message, but it doesn't matter as we just want
        // AsyncOneResponse to signal success, and it only cares about a non-null MessageIn (it collects the payload).
        for (AsyncOneResponse<?> future : resolver.repairResults)
            future.response(MessageIn.create(null, null, null, null, -1));
    }

    @Test
    public void testResolveNewerSingleRow() throws UnknownHostException
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                       .add("c1", "v1")
                                                                                                       .buildUpdate())));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                       .add("c1", "v2")
                                                                                                       .buildUpdate())));

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "c1");
                assertColumn(cfm, row, "c1", "v2", 1);
            }
            assertRepairFuture(resolver, 1);
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
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                       .add("c1", "v1")
                                                                                                       .buildUpdate())));

        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                       .add("c2", "v2")
                                                                                                       .buildUpdate())));

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "c1", "c2");
                assertColumn(cfm, row, "c1", "v1", 0);
                assertColumn(cfm, row, "c2", "v2", 1);
            }
            assertRepairFuture(resolver, 2);
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

        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
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
            assertRepairFuture(resolver, 2);
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
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 4, System.nanoTime());

        RangeTombstone tombstone1 = tombstone("1", "11", 1, nowInSec);
        RangeTombstone tombstone2 = tombstone("3", "31", 1, nowInSec);
        PartitionUpdate update = new RowUpdateBuilder(cfm, nowInSec, 1L, dk).addRangeTombstone(tombstone1)
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
            assertRepairFuture(resolver, 4);
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
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
        InetAddress peer1 = peer();
        resolver.preprocess(readResponseMessage(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                       .add("c2", "v2")
                                                                                                       .buildUpdate())));
        InetAddress peer2 = peer();
        resolver.preprocess(readResponseMessage(peer2, EmptyIterators.unfilteredPartition(cfm, false)));

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "c2");
                assertColumn(cfm, row, "c2", "v2", 1);
            }
            assertRepairFuture(resolver, 1);
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
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
        resolver.preprocess(readResponseMessage(peer(), EmptyIterators.unfilteredPartition(cfm, false)));
        resolver.preprocess(readResponseMessage(peer(), EmptyIterators.unfilteredPartition(cfm, false)));

        try(PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
            assertRepairFuture(resolver, 0);
        }

        assertTrue(messageRecorder.sent.isEmpty());
    }

    @Test
    public void testResolveDeleted()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
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
            assertRepairFuture(resolver, 1);
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
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 4, System.nanoTime());
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

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "two");
                assertColumn(cfm, row, "two", "B", 3);
            }
            assertRepairFuture(resolver, 4);
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

    @Test
    public void testResolveRangeTombstonesOnBoundaryRightWins() throws UnknownHostException
    {
        resolveRangeTombstonesOnBoundary(1, 2);
    }

    @Test
    public void testResolveRangeTombstonesOnBoundaryLeftWins() throws UnknownHostException
    {
        resolveRangeTombstonesOnBoundary(2, 1);
    }

    @Test
    public void testResolveRangeTombstonesOnBoundarySameTimestamp() throws UnknownHostException
    {
        resolveRangeTombstonesOnBoundary(1, 1);
    }

    /*
     * We want responses to merge on tombstone boundary. So we'll merge 2 "streams":
     *   1: [1, 2)(3, 4](5, 6]  2
     *   2:    [2, 3][4, 5)     1
     * which tests all combination of open/close boundaries (open/close, close/open, open/open, close/close).
     *
     * Note that, because DataResolver returns a "filtered" iterator, it should resolve into an empty iterator.
     * However, what should be sent to each source depends on the exact on the timestamps of each tombstones and we
     * test a few combination.
     */
    private void resolveRangeTombstonesOnBoundary(long timestamp1, long timestamp2)
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
        InetAddress peer1 = peer();
        InetAddress peer2 = peer();

        // 1st "stream"
        RangeTombstone one_two    = tombstone("1", true , "2", false, timestamp1, nowInSec);
        RangeTombstone three_four = tombstone("3", false, "4", true , timestamp1, nowInSec);
        RangeTombstone five_six   = tombstone("5", false, "6", true , timestamp1, nowInSec);
        UnfilteredPartitionIterator iter1 = iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).addRangeTombstone(one_two)
                                                                                            .addRangeTombstone(three_four)
                                                                                            .addRangeTombstone(five_six)
                                                                                            .buildUpdate());

        // 2nd "stream"
        RangeTombstone two_three = tombstone("2", true, "3", true , timestamp2, nowInSec);
        RangeTombstone four_five = tombstone("4", true, "5", false, timestamp2, nowInSec);
        UnfilteredPartitionIterator iter2 = iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).addRangeTombstone(two_three)
                                                                                            .addRangeTombstone(four_five)
                                                                                            .buildUpdate());

        resolver.preprocess(readResponseMessage(peer1, iter1));
        resolver.preprocess(readResponseMessage(peer2, iter2));

        // No results, we've only reconciled tombstones.
        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
            assertRepairFuture(resolver, 2);
        }

        assertEquals(2, messageRecorder.sent.size());

        MessageOut msg1 = getSentMessage(peer1);
        assertRepairMetadata(msg1);
        assertRepairContainsNoColumns(msg1);

        MessageOut msg2 = getSentMessage(peer2);
        assertRepairMetadata(msg2);
        assertRepairContainsNoColumns(msg2);

        // Both streams are mostly complementary, so they will roughly get the ranges of the other stream. One subtlety is
        // around the value "4" however, as it's included by both stream.
        // So for a given stream, unless the other stream has a strictly higher timestamp, the value 4 will be excluded
        // from whatever range it receives as repair since the stream already covers it.

        // Message to peer1 contains peer2 ranges
        assertRepairContainsDeletions(msg1, null, two_three, withExclusiveStartIf(four_five, timestamp1 >= timestamp2));

        // Message to peer2 contains peer1 ranges
        assertRepairContainsDeletions(msg2, null, one_two, withExclusiveEndIf(three_four, timestamp2 >= timestamp1), five_six);
    }

    /**
     * Test cases where a boundary of a source is covered by another source deletion and timestamp on one or both side
     * of the boundary are equal to the "merged" deletion.
     * This is a test for CASSANDRA-13237 to make sure we handle this case properly.
     */
    @Test
    public void testRepairRangeTombstoneBoundary() throws UnknownHostException
    {
        testRepairRangeTombstoneBoundary(1, 0, 1);
        messageRecorder.sent.clear();
        testRepairRangeTombstoneBoundary(1, 1, 0);
        messageRecorder.sent.clear();
        testRepairRangeTombstoneBoundary(1, 1, 1);
    }

    /**
     * Test for CASSANDRA-13237, checking we don't fail (and handle correctly) the case where a RT boundary has the
     * same deletion on both side (while is useless but could be created by legacy code pre-CASSANDRA-13237 and could
     * thus still be sent).
     */
    private void testRepairRangeTombstoneBoundary(int timestamp1, int timestamp2, int timestamp3) throws UnknownHostException
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
        InetAddress peer1 = peer();
        InetAddress peer2 = peer();

        // 1st "stream"
        RangeTombstone one_nine = tombstone("0", true , "9", true, timestamp1, nowInSec);
        UnfilteredPartitionIterator iter1 = iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk)
                                                 .addRangeTombstone(one_nine)
                                                 .buildUpdate());

        // 2nd "stream" (build more manually to ensure we have the boundary we want)
        RangeTombstoneBoundMarker open_one = marker("0", true, true, timestamp2, nowInSec);
        RangeTombstoneBoundaryMarker boundary_five = boundary("5", false, timestamp2, nowInSec, timestamp3, nowInSec);
        RangeTombstoneBoundMarker close_nine = marker("9", false, true, timestamp3, nowInSec);
        UnfilteredPartitionIterator iter2 = iter(dk, open_one, boundary_five, close_nine);

        resolver.preprocess(readResponseMessage(peer1, iter1));
        resolver.preprocess(readResponseMessage(peer2, iter2));

        boolean shouldHaveRepair = timestamp1 != timestamp2 || timestamp1 != timestamp3;

        // No results, we've only reconciled tombstones.
        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
            assertRepairFuture(resolver, shouldHaveRepair ? 1 : 0);
        }

        assertEquals(shouldHaveRepair? 1 : 0, messageRecorder.sent.size());

        if (!shouldHaveRepair)
            return;

        MessageOut msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsNoColumns(msg);

        RangeTombstone expected = timestamp1 != timestamp2
                                  // We've repaired the 1st part
                                  ? tombstone("0", true, "5", false, timestamp1, nowInSec)
                                  // We've repaired the 2nd part
                                  : tombstone("5", true, "9", true, timestamp1, nowInSec);
        assertRepairContainsDeletions(msg, null, expected);
    }

    /**
     * Test for CASSANDRA-13719: tests that having a partition deletion shadow a range tombstone on another source
     * doesn't trigger an assertion error.
     */
    @Test
    public void testRepairRangeTombstoneWithPartitionDeletion()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
        InetAddress peer1 = peer();
        InetAddress peer2 = peer();

        // 1st "stream": just a partition deletion
        UnfilteredPartitionIterator iter1 = iter(PartitionUpdate.fullPartitionDelete(cfm, dk, 10, nowInSec));

        // 2nd "stream": a range tombstone that is covered by the 1st stream
        RangeTombstone rt = tombstone("0", true , "10", true, 5, nowInSec);
        UnfilteredPartitionIterator iter2 = iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk)
                                                 .addRangeTombstone(rt)
                                                 .buildUpdate());

        resolver.preprocess(readResponseMessage(peer1, iter1));
        resolver.preprocess(readResponseMessage(peer2, iter2));

        // No results, we've only reconciled tombstones.
        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
            // 2nd stream should get repaired
            assertRepairFuture(resolver, 1);
        }

        assertEquals(1, messageRecorder.sent.size());

        MessageOut msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsNoColumns(msg);

        assertRepairContainsDeletions(msg, new DeletionTime(10, nowInSec));
    }

    /**
     * Additional test for CASSANDRA-13719: tests the case where a partition deletion doesn't shadow a range tombstone.
     */
    @Test
    public void testRepairRangeTombstoneWithPartitionDeletion2()
    {
        DataResolver resolver = new DataResolver(ks, command, ConsistencyLevel.ALL, 2, System.nanoTime());
        InetAddress peer1 = peer();
        InetAddress peer2 = peer();

        // 1st "stream": a partition deletion and a range tombstone
        RangeTombstone rt1 = tombstone("0", true , "9", true, 11, nowInSec);
        PartitionUpdate upd1 = new RowUpdateBuilder(cfm, nowInSec, 1L, dk)
                                                 .addRangeTombstone(rt1)
                                                 .buildUpdate();
        ((MutableDeletionInfo)upd1.deletionInfo()).add(new DeletionTime(10, nowInSec));
        UnfilteredPartitionIterator iter1 = iter(upd1);

        // 2nd "stream": a range tombstone that is covered by the other stream rt
        RangeTombstone rt2 = tombstone("2", true , "3", true, 11, nowInSec);
        RangeTombstone rt3 = tombstone("4", true , "5", true, 10, nowInSec);
        UnfilteredPartitionIterator iter2 = iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk)
                                                 .addRangeTombstone(rt2)
                                                 .addRangeTombstone(rt3)
                                                 .buildUpdate());

        resolver.preprocess(readResponseMessage(peer1, iter1));
        resolver.preprocess(readResponseMessage(peer2, iter2));

        // No results, we've only reconciled tombstones.
        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
            // 2nd stream should get repaired
            assertRepairFuture(resolver, 1);
        }

        assertEquals(1, messageRecorder.sent.size());

        MessageOut msg = getSentMessage(peer2);
        assertRepairMetadata(msg);
        assertRepairContainsNoColumns(msg);

        // 2nd stream should get both the partition deletion, as well as the part of the 1st stream RT that it misses
        assertRepairContainsDeletions(msg, new DeletionTime(10, nowInSec),
                                      tombstone("0", true, "2", false, 11, nowInSec),
                                      tombstone("3", false, "9", true, 11, nowInSec));
    }

    // Forces the start to be exclusive if the condition holds
    private static RangeTombstone withExclusiveStartIf(RangeTombstone rt, boolean condition)
    {
        if (!condition)
            return rt;

        Slice slice = rt.deletedSlice();
        ClusteringBound newStart = ClusteringBound.create(Kind.EXCL_START_BOUND, slice.start().getRawValues());
        return condition
             ? new RangeTombstone(Slice.make(newStart, slice.end()), rt.deletionTime())
             : rt;
    }

    // Forces the end to be exclusive if the condition holds
    private static RangeTombstone withExclusiveEndIf(RangeTombstone rt, boolean condition)
    {
        if (!condition)
            return rt;

        Slice slice = rt.deletedSlice();
        ClusteringBound newEnd = ClusteringBound.create(Kind.EXCL_END_BOUND, slice.end().getRawValues());
        return condition
             ? new RangeTombstone(Slice.make(slice.start(), newEnd), rt.deletionTime())
             : rt;
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
        DataResolver resolver = new DataResolver(ks, cmd, ConsistencyLevel.ALL, 2, System.nanoTime());

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

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "m");
                Assert.assertNull(row.getCell(m, CellPath.create(bb(0))));
                Assert.assertNotNull(row.getCell(m, CellPath.create(bb(1))));
            }
            assertRepairFuture(resolver, 1);
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
        DataResolver resolver = new DataResolver(ks, cmd, ConsistencyLevel.ALL, 2, System.nanoTime());

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
            assertRepairFuture(resolver, 1);
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
        DataResolver resolver = new DataResolver(ks, cmd, ConsistencyLevel.ALL, 2, System.nanoTime());

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

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "m");
                ComplexColumnData cd = row.getComplexColumnData(m);
                assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
            }
            assertRepairFuture(resolver, 1);
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
        DataResolver resolver = new DataResolver(ks, cmd, ConsistencyLevel.ALL, 2, System.nanoTime());

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

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "m");
                ComplexColumnData cd = row.getComplexColumnData(m);
                assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
            }
            assertRepairFuture(resolver, 1);
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
            RangeTombstone expected = rangeTombstones[i++];
            RangeTombstone actual = ranges.next();
            String msg = String.format("Expected %s, but got %s", expected.toString(cfm.comparator), actual.toString(cfm.comparator));
            assertEquals(msg, expected, actual);
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
                                MessagingService.current_version);
    }

    private RangeTombstone tombstone(Object start, Object end, long markedForDeleteAt, int localDeletionTime)
    {
        return tombstone(start, true, end, true, markedForDeleteAt, localDeletionTime);
    }

    private RangeTombstone tombstone(Object start, boolean inclusiveStart, Object end, boolean inclusiveEnd, long markedForDeleteAt, int localDeletionTime)
    {
        ClusteringBound startBound = rtBound(start, true, inclusiveStart);
        ClusteringBound endBound = rtBound(end, false, inclusiveEnd);
        return new RangeTombstone(Slice.make(startBound, endBound), new DeletionTime(markedForDeleteAt, localDeletionTime));
    }

    private ClusteringBound rtBound(Object value, boolean isStart, boolean inclusive)
    {
        ClusteringBound.Kind kind = isStart
                                         ? (inclusive ? Kind.INCL_START_BOUND : Kind.EXCL_START_BOUND)
                                         : (inclusive ? Kind.INCL_END_BOUND : Kind.EXCL_END_BOUND);

        return ClusteringBound.create(kind, cfm.comparator.make(value).getRawValues());
    }

    private ClusteringBoundary rtBoundary(Object value, boolean inclusiveOnEnd)
    {
        ClusteringBound.Kind kind = inclusiveOnEnd
                                         ? Kind.INCL_END_EXCL_START_BOUNDARY
                                         : Kind.EXCL_END_INCL_START_BOUNDARY;
        return ClusteringBoundary.create(kind, cfm.comparator.make(value).getRawValues());
    }

    private RangeTombstoneBoundMarker marker(Object value, boolean isStart, boolean inclusive, long markedForDeleteAt, int localDeletionTime)
    {
        return new RangeTombstoneBoundMarker(rtBound(value, isStart, inclusive), new DeletionTime(markedForDeleteAt, localDeletionTime));
    }

    private RangeTombstoneBoundaryMarker boundary(Object value, boolean inclusiveOnEnd, long markedForDeleteAt1, int localDeletionTime1, long markedForDeleteAt2, int localDeletionTime2)
    {
        return new RangeTombstoneBoundaryMarker(rtBoundary(value, inclusiveOnEnd),
                                                new DeletionTime(markedForDeleteAt1, localDeletionTime1),
                                                new DeletionTime(markedForDeleteAt2, localDeletionTime2));
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

    private UnfilteredPartitionIterator iter(DecoratedKey key, Unfiltered... unfiltereds)
    {
        SortedSet<Unfiltered> s = new TreeSet<>(cfm.comparator);
        Collections.addAll(s, unfiltereds);
        final Iterator<Unfiltered> iterator = s.iterator();

        UnfilteredRowIterator rowIter = new AbstractUnfilteredRowIterator(cfm,
                                                                          key,
                                                                          DeletionTime.LIVE,
                                                                          cfm.partitionColumns(),
                                                                          Rows.EMPTY_STATIC_ROW,
                                                                          false,
                                                                          EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                return iterator.hasNext() ? iterator.next() : endOfData();
            }
        };
        return new SingletonUnfilteredPartitionIterator(rowIter, false);
    }
}
