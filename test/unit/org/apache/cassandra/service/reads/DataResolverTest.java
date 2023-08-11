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
package org.apache.cassandra.service.reads;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.service.reads.repair.RepairedDataTracker;
import org.apache.cassandra.service.reads.repair.RepairedDataVerifier;
import org.apache.cassandra.service.reads.repair.TestableReadRepair;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.assertClustering;
import static org.apache.cassandra.Util.assertColumn;
import static org.apache.cassandra.Util.assertColumns;
import static org.apache.cassandra.db.ClusteringBound.Kind;
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataResolverTest extends AbstractReadResponseTest
{
    private ReadCommand command;
    private TestableReadRepair readRepair;
    private Keyspace ks;
    private ColumnFamilyStore cfs;

    private EndpointsForRange makeReplicas(int num)
    {
        StorageService.instance.getTokenMetadata().clearUnsafe();

        switch (num)
        {
            case 2:
                ks = AbstractReadResponseTest.ks;
                cfs = AbstractReadResponseTest.cfs;
                break;
            case 4:
                ks = AbstractReadResponseTest.ks3;
                cfs = AbstractReadResponseTest.cfs3;
                break;
            default:
                throw new IllegalStateException("This test needs refactoring to cleanly support different replication factors");
        }

        command = Util.cmd(cfs, dk).withNowInSeconds(nowInSec).build();
        readRepair = new TestableReadRepair(command);
        Token token = Murmur3Partitioner.instance.getMinimumToken();
        EndpointsForRange.Builder replicas = EndpointsForRange.builder(ReplicaUtils.FULL_RANGE, num);
        for (int i = 0; i < num; i++)
        {
            try
            {
                InetAddressAndPort endpoint = InetAddressAndPort.getByAddress(new byte[]{ 127, 0, 0, (byte) (i + 1) });
                replicas.add(ReplicaUtils.full(endpoint));
                StorageService.instance.getTokenMetadata().updateNormalToken(token = token.nextValidToken(), endpoint);
                Gossiper.instance.initializeNodeUnsafe(endpoint, UUID.randomUUID(), 1);
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            }
        }
        return replicas.build();
    }

    @Test
    public void testResolveNewerSingleRow()
    {
        EndpointsForRange replicas = makeReplicas(2);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(command, peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                     .add("c1", "v1")
                                                                                                     .buildUpdate()), false));
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(command, peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                     .add("c1", "v2")
                                                                                                     .buildUpdate()), false));

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "c1");
                assertColumn(cfm, row, "c1", "v2", 1);
            }
        }

        assertEquals(1, readRepair.sent.size());
        // peer 1 just needs to repair with the row from peer 2
        Mutation mutation = readRepair.getForEndpoint(peer1);
        assertRepairMetadata(mutation);
        assertRepairContainsNoDeletions(mutation);
        assertRepairContainsColumn(mutation, "1", "c1", "v2", 1);
    }

    @Test
    public void testResolveDisjointSingleRow()
    {
        EndpointsForRange replicas = makeReplicas(2);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(command, peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                     .add("c1", "v1")
                                                                                                     .buildUpdate())));

        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(command, peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
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
        }

        assertEquals(2, readRepair.sent.size());
        // each peer needs to repair with each other's column
        Mutation mutation = readRepair.getForEndpoint(peer1);
        assertRepairMetadata(mutation);
        assertRepairContainsColumn(mutation, "1", "c2", "v2", 1);

        mutation = readRepair.getForEndpoint(peer2);
        assertRepairMetadata(mutation);
        assertRepairContainsColumn(mutation, "1", "c1", "v1", 0);
    }

    @Test
    public void testResolveDisjointMultipleRows() throws UnknownHostException
    {
        EndpointsForRange replicas = makeReplicas(2);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(command, peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                     .add("c1", "v1")
                                                                                                     .buildUpdate())));
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(command, peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("2")
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

        assertEquals(2, readRepair.sent.size());
        // each peer needs to repair the row from the other
        Mutation mutation = readRepair.getForEndpoint(peer1);
        assertRepairMetadata(mutation);
        assertRepairContainsNoDeletions(mutation);
        assertRepairContainsColumn(mutation, "2", "c2", "v2", 1);

        mutation = readRepair.getForEndpoint(peer2);
        assertRepairMetadata(mutation);
        assertRepairContainsNoDeletions(mutation);
        assertRepairContainsColumn(mutation, "1", "c1", "v1", 0);
    }

    @Test
    public void testResolveDisjointMultipleRowsWithRangeTombstones()
    {
        EndpointsForRange replicas = makeReplicas(4);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());

        RangeTombstone tombstone1 = tombstone("1", "11", 1, nowInSec);
        RangeTombstone tombstone2 = tombstone("3", "31", 1, nowInSec);
        PartitionUpdate update = new RowUpdateBuilder(cfm3, nowInSec, 1L, dk).addRangeTombstone(tombstone1)
                                                                            .addRangeTombstone(tombstone2)
                                                                            .buildUpdate();

        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        UnfilteredPartitionIterator iter1 = iter(new RowUpdateBuilder(cfm3, nowInSec, 1L, dk).addRangeTombstone(tombstone1)
                                                                                            .addRangeTombstone(tombstone2)
                                                                                            .buildUpdate());
        resolver.preprocess(response(command, peer1, iter1));
        // not covered by any range tombstone
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        UnfilteredPartitionIterator iter2 = iter(new RowUpdateBuilder(cfm3, nowInSec, 0L, dk).clustering("0")
                                                                                            .add("c1", "v0")
                                                                                            .buildUpdate());
        resolver.preprocess(response(command, peer2, iter2));
        // covered by a range tombstone
        InetAddressAndPort peer3 = replicas.get(2).endpoint();
        UnfilteredPartitionIterator iter3 = iter(new RowUpdateBuilder(cfm3, nowInSec, 0L, dk).clustering("10")
                                                                                            .add("c2", "v1")
                                                                                            .buildUpdate());
        resolver.preprocess(response(command, peer3, iter3));
        // range covered by rt, but newer
        InetAddressAndPort peer4 = replicas.get(3).endpoint();
        UnfilteredPartitionIterator iter4 = iter(new RowUpdateBuilder(cfm3, nowInSec, 2L, dk).clustering("3")
                                                                                            .add("one", "A")
                                                                                            .buildUpdate());
        resolver.preprocess(response(command, peer4, iter4));
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

        assertEquals(4, readRepair.sent.size());
        // peer1 needs the rows from peers 2 and 4
        Mutation mutation = readRepair.getForEndpoint(peer1);
        assertRepairMetadata(mutation);
        assertRepairContainsNoDeletions(mutation);
        assertRepairContainsColumn(mutation, "0", "c1", "v0", 0);
        assertRepairContainsColumn(mutation, "3", "one", "A", 2);

        // peer2 needs to get the row from peer4 and the RTs
        mutation = readRepair.getForEndpoint(peer2);
        assertRepairMetadata(mutation);
        assertRepairContainsDeletions(mutation, null, tombstone1, tombstone2);
        assertRepairContainsColumn(mutation, "3", "one", "A", 2);

        // peer 3 needs both rows and the RTs
        mutation = readRepair.getForEndpoint(peer3);
        assertRepairMetadata(mutation);
        assertRepairContainsDeletions(mutation, null, tombstone1, tombstone2);
        assertRepairContainsColumn(mutation, "0", "c1", "v0", 0);
        assertRepairContainsColumn(mutation, "3", "one", "A", 2);

        // peer4 needs the row from peer2  and the RTs
        mutation = readRepair.getForEndpoint(peer4);
        assertRepairMetadata(mutation);
        assertRepairContainsDeletions(mutation, null, tombstone1, tombstone2);
        assertRepairContainsColumn(mutation, "0", "c1", "v0", 0);
    }

    @Test
    public void testResolveWithOneEmpty()
    {
        EndpointsForRange replicas = makeReplicas(2);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(command, peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                     .add("c2", "v2")
                                                                                                     .buildUpdate())));
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(command, peer2, EmptyIterators.unfilteredPartition(cfm)));

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "c2");
                assertColumn(cfm, row, "c2", "v2", 1);
            }
        }

        assertEquals(1, readRepair.sent.size());
        // peer 2 needs the row from peer 1
        Mutation mutation = readRepair.getForEndpoint(peer2);
        assertRepairMetadata(mutation);
        assertRepairContainsNoDeletions(mutation);
        assertRepairContainsColumn(mutation, "1", "c2", "v2", 1);
    }

    @Test
    public void testResolveWithBothEmpty()
    {
        EndpointsForRange replicas = makeReplicas(2);
        TestableReadRepair readRepair = new TestableReadRepair(command);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        resolver.preprocess(response(command, replicas.get(0).endpoint(), EmptyIterators.unfilteredPartition(cfm)));
        resolver.preprocess(response(command, replicas.get(1).endpoint(), EmptyIterators.unfilteredPartition(cfm)));

        try(PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        assertTrue(readRepair.sent.isEmpty());
    }

    @Test
    public void testResolveDeleted()
    {
        EndpointsForRange replicas = makeReplicas(2);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        // one response with columns timestamped before a delete in another response
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(command, peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 0L, dk).clustering("1")
                                                                                                     .add("one", "A")
                                                                                                     .buildUpdate())));
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(command, peer2, fullPartitionDelete(cfm, dk, 1, nowInSec)));

        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        // peer1 should get the deletion from peer2
        assertEquals(1, readRepair.sent.size());
        Mutation mutation = readRepair.getForEndpoint(peer1);
        assertRepairMetadata(mutation);
        assertRepairContainsDeletions(mutation, DeletionTime.build(1, nowInSec));
        assertRepairContainsNoColumns(mutation);
    }

    @Test
    public void testResolveMultipleDeleted()
    {
        EndpointsForRange replicas = makeReplicas(4);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        // deletes and columns with interleaved timestamp, with out of order return sequence
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(command, peer1, fullPartitionDelete(cfm, dk, 0, nowInSec)));
        // these columns created after the previous deletion
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(command, peer2, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1")
                                                                                                     .add("one", "A")
                                                                                                     .add("two", "A")
                                                                                                     .buildUpdate())));
        //this column created after the next delete
        InetAddressAndPort peer3 = replicas.get(2).endpoint();
        resolver.preprocess(response(command, peer3, iter(new RowUpdateBuilder(cfm, nowInSec, 3L, dk).clustering("1")
                                                                                                     .add("two", "B")
                                                                                                     .buildUpdate())));
        InetAddressAndPort peer4 = replicas.get(3).endpoint();
        resolver.preprocess(response(command, peer4, fullPartitionDelete(cfm, dk, 2, nowInSec)));

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "two");
                assertColumn(cfm, row, "two", "B", 3);
            }
        }

        // peer 1 needs to get the partition delete from peer 4 and the row from peer 3
        assertEquals(4, readRepair.sent.size());
        Mutation mutation = readRepair.getForEndpoint(peer1);
        assertRepairMetadata(mutation);
        assertRepairContainsDeletions(mutation, DeletionTime.build(2, nowInSec));
        assertRepairContainsColumn(mutation, "1", "two", "B", 3);

        // peer 2 needs the deletion from peer 4 and the row from peer 3
        mutation = readRepair.getForEndpoint(peer2);
        assertRepairMetadata(mutation);
        assertRepairContainsDeletions(mutation, DeletionTime.build(2, nowInSec));
        assertRepairContainsColumn(mutation, "1", "two", "B", 3);

        // peer 3 needs just the deletion from peer 4
        mutation = readRepair.getForEndpoint(peer3);
        assertRepairMetadata(mutation);
        assertRepairContainsDeletions(mutation, DeletionTime.build(2, nowInSec));
        assertRepairContainsNoColumns(mutation);

        // peer 4 needs just the row from peer 3
        mutation = readRepair.getForEndpoint(peer4);
        assertRepairMetadata(mutation);
        assertRepairContainsNoDeletions(mutation);
        assertRepairContainsColumn(mutation, "1", "two", "B", 3);
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
        EndpointsForRange replicas = makeReplicas(2);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();

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

        resolver.preprocess(response(command, peer1, iter1));
        resolver.preprocess(response(command, peer2, iter2));

        // No results, we've only reconciled tombstones.
        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        assertEquals(2, readRepair.sent.size());

        Mutation msg1 = readRepair.getForEndpoint(peer1);
        assertRepairMetadata(msg1);
        assertRepairContainsNoColumns(msg1);

        Mutation msg2 = readRepair.getForEndpoint(peer2);
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
        readRepair.sent.clear();
        testRepairRangeTombstoneBoundary(1, 1, 0);
        readRepair.sent.clear();
        testRepairRangeTombstoneBoundary(1, 1, 1);
    }

    /**
     * Test for CASSANDRA-13237, checking we don't fail (and handle correctly) the case where a RT boundary has the
     * same deletion on both side (while is useless but could be created by legacy code pre-CASSANDRA-13237 and could
     * thus still be sent).
     */
    private void testRepairRangeTombstoneBoundary(int timestamp1, int timestamp2, int timestamp3) throws UnknownHostException
    {
        EndpointsForRange replicas = makeReplicas(2);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();

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

        resolver.preprocess(response(command, peer1, iter1));
        resolver.preprocess(response(command, peer2, iter2));

        boolean shouldHaveRepair = timestamp1 != timestamp2 || timestamp1 != timestamp3;

        // No results, we've only reconciled tombstones.
        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        assertEquals(shouldHaveRepair? 1 : 0, readRepair.sent.size());

        if (!shouldHaveRepair)
            return;

        Mutation mutation = readRepair.getForEndpoint(peer2);
        assertRepairMetadata(mutation);
        assertRepairContainsNoColumns(mutation);

        RangeTombstone expected = timestamp1 != timestamp2
                                  // We've repaired the 1st part
                                  ? tombstone("0", true, "5", false, timestamp1, nowInSec)
                                  // We've repaired the 2nd part
                                  : tombstone("5", true, "9", true, timestamp1, nowInSec);
        assertRepairContainsDeletions(mutation, null, expected);
    }

    /**
     * Test for CASSANDRA-13719: tests that having a partition deletion shadow a range tombstone on another source
     * doesn't trigger an assertion error.
     */
    @Test
    public void testRepairRangeTombstoneWithPartitionDeletion()
    {
        EndpointsForRange replicas = makeReplicas(2);

        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();

        // 1st "stream": just a partition deletion
        UnfilteredPartitionIterator iter1 = iter(PartitionUpdate.fullPartitionDelete(cfm, dk, 10, nowInSec));

        // 2nd "stream": a range tombstone that is covered by the 1st stream
        RangeTombstone rt = tombstone("0", true , "10", true, 5, nowInSec);
        UnfilteredPartitionIterator iter2 = iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk)
                                                 .addRangeTombstone(rt)
                                                 .buildUpdate());

        resolver.preprocess(response(command, peer1, iter1));
        resolver.preprocess(response(command, peer2, iter2));

        // No results, we've only reconciled tombstones.
        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
            // 2nd stream should get repaired
        }

        assertEquals(1, readRepair.sent.size());

        Mutation mutation = readRepair.getForEndpoint(peer2);
        assertRepairMetadata(mutation);
        assertRepairContainsNoColumns(mutation);

        assertRepairContainsDeletions(mutation, DeletionTime.build(10, nowInSec));
    }

    /**
     * Additional test for CASSANDRA-13719: tests the case where a partition deletion doesn't shadow a range tombstone.
     */
    @Test
    public void testRepairRangeTombstoneWithPartitionDeletion2()
    {
        EndpointsForRange replicas = makeReplicas(2);
        DataResolver resolver = new DataResolver(command, plan(replicas, ALL), readRepair, nanoTime());
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();

        // 1st "stream": a partition deletion and a range tombstone
        RangeTombstone rt1 = tombstone("0", true , "9", true, 11, nowInSec);
        PartitionUpdate upd1 = new RowUpdateBuilder(cfm, nowInSec, 1L, dk)
                               .addRangeTombstone(rt1)
                               .buildUpdate();
        ((MutableDeletionInfo)upd1.deletionInfo()).add(DeletionTime.build(10, nowInSec));
        UnfilteredPartitionIterator iter1 = iter(upd1);

        // 2nd "stream": a range tombstone that is covered by the other stream rt
        RangeTombstone rt2 = tombstone("2", true , "3", true, 11, nowInSec);
        RangeTombstone rt3 = tombstone("4", true , "5", true, 10, nowInSec);
        UnfilteredPartitionIterator iter2 = iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk)
                                                 .addRangeTombstone(rt2)
                                                 .addRangeTombstone(rt3)
                                                 .buildUpdate());

        resolver.preprocess(response(command, peer1, iter1));
        resolver.preprocess(response(command, peer2, iter2));

        // No results, we've only reconciled tombstones.
        try (PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
            // 2nd stream should get repaired
        }

        assertEquals(1, readRepair.sent.size());

        Mutation mutation = readRepair.getForEndpoint(peer2);
        assertRepairMetadata(mutation);
        assertRepairContainsNoColumns(mutation);

        // 2nd stream should get both the partition deletion, as well as the part of the 1st stream RT that it misses
        assertRepairContainsDeletions(mutation, DeletionTime.build(10, nowInSec),
                                      tombstone("0", true, "2", false, 11, nowInSec),
                                      tombstone("3", false, "9", true, 11, nowInSec));
    }

    // Forces the start to be exclusive if the condition holds
    private static RangeTombstone withExclusiveStartIf(RangeTombstone rt, boolean condition)
    {
        if (!condition)
            return rt;

        Slice slice = rt.deletedSlice();
        ClusteringBound<?> newStart = ClusteringBound.create(Kind.EXCL_START_BOUND, slice.start());
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
        ClusteringBound<?> newEnd = ClusteringBound.create(Kind.EXCL_END_BOUND, slice.end());
        return condition
               ? new RangeTombstone(Slice.make(slice.start(), newEnd), rt.deletionTime())
               : rt;
    }

    private static ByteBuffer bb(int b)
    {
        return ByteBufferUtil.bytes(b);
    }

    private Cell<?> mapCell(int k, int v, long ts)
    {
        return BufferCell.live(m, ts, bb(v), CellPath.create(bb(k)));
    }

    @Test
    public void testResolveComplexDelete()
    {
        EndpointsForRange replicas = makeReplicas(2);
        ReadCommand cmd = Util.cmd(cfs2, dk).withNowInSeconds(nowInSec).build();
        TestableReadRepair readRepair = new TestableReadRepair(cmd);
        DataResolver resolver = new DataResolver(cmd, plan(replicas, ALL), readRepair, nanoTime());

        long[] ts = {100, 200};

        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addComplexDeletion(m, DeletionTime.build(ts[0] - 1, nowInSec));
        builder.addCell(mapCell(0, 0, ts[0]));

        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(cmd, peer1, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build()))));

        builder.newRow(Clustering.EMPTY);
        DeletionTime expectedCmplxDelete = DeletionTime.build(ts[1] - 1, nowInSec);
        builder.addComplexDeletion(m, expectedCmplxDelete);
        Cell<?> expectedCell = mapCell(1, 1, ts[1]);
        builder.addCell(expectedCell);

        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(cmd, peer2, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build()))));

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "m");
                Assert.assertNull(row.getCell(m, CellPath.create(bb(0))));
                Assert.assertNotNull(row.getCell(m, CellPath.create(bb(1))));
            }
        }

        Mutation mutation = readRepair.getForEndpoint(peer1);
        Iterator<Row> rowIter = mutation.getPartitionUpdate(cfm2).iterator();
        assertTrue(rowIter.hasNext());
        Row row = rowIter.next();
        assertFalse(rowIter.hasNext());

        ComplexColumnData cd = row.getComplexColumnData(m);

        assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
        assertEquals(expectedCmplxDelete, cd.complexDeletion());

        Assert.assertNull(readRepair.sent.get(peer2));
    }

    @Test
    public void testResolveDeletedCollection()
    {
        EndpointsForRange replicas = makeReplicas(2);
        ReadCommand cmd = Util.cmd(cfs2, dk).withNowInSeconds(nowInSec).build();
        TestableReadRepair readRepair = new TestableReadRepair(cmd);
        DataResolver resolver = new DataResolver(cmd, plan(replicas, ALL), readRepair, nanoTime());

        long[] ts = {100, 200};

        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addComplexDeletion(m, DeletionTime.build(ts[0] - 1, nowInSec));
        builder.addCell(mapCell(0, 0, ts[0]));

        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(cmd, peer1, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build()))));

        builder.newRow(Clustering.EMPTY);
        DeletionTime expectedCmplxDelete = DeletionTime.build(ts[1] - 1, nowInSec);
        builder.addComplexDeletion(m, expectedCmplxDelete);

        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(cmd, peer2, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build()))));

        try(PartitionIterator data = resolver.resolve())
        {
            assertFalse(data.hasNext());
        }

        Mutation mutation = readRepair.getForEndpoint(peer1);
        Iterator<Row> rowIter = mutation.getPartitionUpdate(cfm2).iterator();
        assertTrue(rowIter.hasNext());
        Row row = rowIter.next();
        assertFalse(rowIter.hasNext());

        ComplexColumnData cd = row.getComplexColumnData(m);

        assertEquals(Collections.emptySet(), Sets.newHashSet(cd));
        assertEquals(expectedCmplxDelete, cd.complexDeletion());

        Assert.assertNull(readRepair.sent.get(peer2));
    }

    @Test
    public void testResolveNewCollection()
    {
        EndpointsForRange replicas = makeReplicas(2);
        ReadCommand cmd = Util.cmd(cfs2, dk).withNowInSeconds(nowInSec).build();
        TestableReadRepair readRepair = new TestableReadRepair(cmd);
        DataResolver resolver = new DataResolver(cmd, plan(replicas, ALL), readRepair, nanoTime());

        long[] ts = {100, 200};

        // map column
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        DeletionTime expectedCmplxDelete = DeletionTime.build(ts[0] - 1, nowInSec);
        builder.addComplexDeletion(m, expectedCmplxDelete);
        Cell<?> expectedCell = mapCell(0, 0, ts[0]);
        builder.addCell(expectedCell);

        // empty map column
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(cmd, peer1, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build()))));

        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(cmd, peer2, iter(PartitionUpdate.emptyUpdate(cfm2, dk))));

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "m");
                ComplexColumnData cd = row.getComplexColumnData(m);
                assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
            }
        }

        Assert.assertNull(readRepair.sent.get(peer1));

        Mutation mutation = readRepair.getForEndpoint(peer2);
        Iterator<Row> rowIter = mutation.getPartitionUpdate(cfm2).iterator();
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
        EndpointsForRange replicas = makeReplicas(2);
        ReadCommand cmd = Util.cmd(cfs2, dk).withNowInSeconds(nowInSec).build();
        TestableReadRepair readRepair = new TestableReadRepair(cmd);
        DataResolver resolver = new DataResolver(cmd, plan(replicas, ALL), readRepair, nanoTime());

        long[] ts = {100, 200};

        // cleared map column
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addComplexDeletion(m, DeletionTime.build(ts[0] - 1, nowInSec));

        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        resolver.preprocess(response(cmd, peer1, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build()))));

        // newer, overwritten map column
        builder.newRow(Clustering.EMPTY);
        DeletionTime expectedCmplxDelete = DeletionTime.build(ts[1] - 1, nowInSec);
        builder.addComplexDeletion(m, expectedCmplxDelete);
        Cell<?> expectedCell = mapCell(1, 1, ts[1]);
        builder.addCell(expectedCell);

        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        resolver.preprocess(response(cmd, peer2, iter(PartitionUpdate.singleRowUpdate(cfm2, dk, builder.build()))));

        try(PartitionIterator data = resolver.resolve())
        {
            try (RowIterator rows = Iterators.getOnlyElement(data))
            {
                Row row = Iterators.getOnlyElement(rows);
                assertColumns(row, "m");
                ComplexColumnData cd = row.getComplexColumnData(m);
                assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
            }
        }

        Row row = Iterators.getOnlyElement(readRepair.getForEndpoint(peer1).getPartitionUpdate(cfm2).iterator());

        ComplexColumnData cd = row.getComplexColumnData(m);

        assertEquals(Collections.singleton(expectedCell), Sets.newHashSet(cd));
        assertEquals(expectedCmplxDelete, cd.complexDeletion());

        Assert.assertNull(readRepair.sent.get(peer2));
    }

    /** Tests for repaired data tracking */

    @Test
    public void trackMatchingEmptyDigestsWithAllConclusive()
    {
        EndpointsForRange replicas = makeReplicas(2);
        ByteBuffer digest1 = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, true);
        verifier.expectDigest(peer2, digest1, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, true, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, true, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMatchingEmptyDigestsWithSomeConclusive()
    {
        ByteBuffer digest1 = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        EndpointsForRange replicas = makeReplicas(2);
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, false);
        verifier.expectDigest(peer2, digest1, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, false, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, true, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMatchingEmptyDigestsWithNoneConclusive()
    {
        ByteBuffer digest1 = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        EndpointsForRange replicas = makeReplicas(2);
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, false);
        verifier.expectDigest(peer2, digest1, false);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, false, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, false, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMatchingDigestsWithAllConclusive()
    {
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        EndpointsForRange replicas = makeReplicas(2);
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, true);
        verifier.expectDigest(peer2, digest1, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm,dk)), digest1, true, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, true, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMatchingDigestsWithSomeConclusive()
    {
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        EndpointsForRange replicas = makeReplicas(2);
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, true);
        verifier.expectDigest(peer2, digest1, false);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm,dk)), digest1, true, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, false, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMatchingDigestsWithNoneConclusive()
    {
        EndpointsForRange replicas = makeReplicas(2);
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, false);
        verifier.expectDigest(peer2, digest1, false);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, false, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, false, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMatchingRepairedDigestsWithDifferentData()
    {
        // As far as repaired data tracking is concerned, the actual data in the response is not relevant
        EndpointsForRange replicas = makeReplicas(2);
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, true);
        verifier.expectDigest(peer2, digest1, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1") .buildUpdate()), digest1, true, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, true, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMismatchingRepairedDigestsWithAllConclusive()
    {
        EndpointsForRange replicas = makeReplicas(2);
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        ByteBuffer digest2 = ByteBufferUtil.bytes("digest2");
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, true);
        verifier.expectDigest(peer2, digest2, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, true, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest2, true, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMismatchingRepairedDigestsWithSomeConclusive()
    {
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        ByteBuffer digest2 = ByteBufferUtil.bytes("digest2");
        EndpointsForRange replicas = makeReplicas(2);
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, false);
        verifier.expectDigest(peer2, digest2, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, false, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest2, true, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMismatchingRepairedDigestsWithNoneConclusive()
    {
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        ByteBuffer digest2 = ByteBufferUtil.bytes("digest2");
        EndpointsForRange replicas = makeReplicas(2);
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, false);
        verifier.expectDigest(peer2, digest2, false);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest1, false, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest2, false, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void trackMismatchingRepairedDigestsWithDifferentData()
    {
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        ByteBuffer digest2 = ByteBufferUtil.bytes("digest2");
        EndpointsForRange replicas = makeReplicas(2);
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, true);
        verifier.expectDigest(peer2, digest2, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(new RowUpdateBuilder(cfm, nowInSec, 1L, dk).clustering("1") .buildUpdate()), digest1, true, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm, dk)), digest2, true, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void noVerificationForSingletonResponse()
    {
        // for CL <= 1 a coordinator shouldn't request repaired data tracking but we
        // can easily assert that the verification isn't attempted even if it did
        EndpointsForRange replicas = makeReplicas(2);
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        verifier.expectDigest(peer1, digest1, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm,dk)), digest1, true, command));

        resolveAndConsume(resolver);
        assertFalse(verifier.verified);
    }

    @Test
    public void responsesFromOlderVersionsAreNotTracked()
    {
        // In a mixed version cluster, responses from a replicas running older versions won't include
        // tracking info, so the digest and pending session status are defaulted. To make sure these
        // default values don't result in false positives we make sure not to consider them when
        // processing in DataResolver
        EndpointsForRange replicas = makeReplicas(2);
        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        verifier.expectDigest(peer1, digest1, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm,dk)), digest1, true, command));
        // peer2 is advertising an older version, so when we deserialize its response there are two things to note:
        // i) the actual serialized response cannot contain any tracking info so deserialization will use defaults of
        //    an empty digest and pending sessions = false
        // ii) under normal circumstances, this would cause a mismatch with peer1, but because of the older version,
        //     here it will not
        resolver.preprocess(response(command, peer2, iter(PartitionUpdate.emptyUpdate(cfm,dk)),
                                     false, MessagingService.VERSION_30,
                                     ByteBufferUtil.EMPTY_BYTE_BUFFER, false));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    @Test
    public void responsesFromTransientReplicasAreNotTracked()
    {
        EndpointsForRange replicas = makeReplicas(2);
        EndpointsForRange.Builder mutable = replicas.newBuilder(2);
        mutable.add(replicas.get(0));
        mutable.add(Replica.transientReplica(replicas.get(1).endpoint(), replicas.range()));
        replicas = mutable.build();

        TestRepairedDataVerifier verifier = new TestRepairedDataVerifier();
        ByteBuffer digest1 = ByteBufferUtil.bytes("digest1");
        ByteBuffer digest2 = ByteBufferUtil.bytes("digest2");
        InetAddressAndPort peer1 = replicas.get(0).endpoint();
        InetAddressAndPort peer2 = replicas.get(1).endpoint();
        verifier.expectDigest(peer1, digest1, true);

        DataResolver resolver = resolverWithVerifier(command, plan(replicas, ALL), readRepair, nanoTime(), verifier);

        resolver.preprocess(response(peer1, iter(PartitionUpdate.emptyUpdate(cfm,dk)), digest1, true, command));
        resolver.preprocess(response(peer2, iter(PartitionUpdate.emptyUpdate(cfm,dk)), digest2, true, command));

        resolveAndConsume(resolver);
        assertTrue(verifier.verified);
    }

    private static class TestRepairedDataVerifier implements RepairedDataVerifier
    {
        private final RepairedDataTracker expected = new RepairedDataTracker(null);
        private boolean verified = false;

        private void expectDigest(InetAddressAndPort from, ByteBuffer digest, boolean conclusive)
        {
            expected.recordDigest(from, digest, conclusive);
        }

        @Override
        public void verify(RepairedDataTracker tracker)
        {
            verified = expected.equals(tracker);
        }
    }

    private DataResolver resolverWithVerifier(final ReadCommand command,
                                              final ReplicaPlan.SharedForRangeRead plan,
                                              final ReadRepair readRepair,
                                              final long queryStartNanoTime,
                                              final RepairedDataVerifier verifier)
    {
        class TestableDataResolver extends DataResolver
        {

            public TestableDataResolver(ReadCommand command, ReplicaPlan.SharedForRangeRead plan, ReadRepair readRepair, long queryStartNanoTime)
            {
                super(command, plan, readRepair, queryStartNanoTime, true);
            }

            protected RepairedDataVerifier getRepairedDataVerifier(ReadCommand command)
            {
                return verifier;
            }
        }

        return new TestableDataResolver(command, plan, readRepair, queryStartNanoTime);
    }

    private void assertRepairContainsDeletions(Mutation mutation,
                                               DeletionTime deletionTime,
                                               RangeTombstone...rangeTombstones)
    {
        PartitionUpdate update = mutation.getPartitionUpdates().iterator().next();
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

    private void assertRepairContainsNoDeletions(Mutation mutation)
    {
        PartitionUpdate update = mutation.getPartitionUpdates().iterator().next();
        assertTrue(update.deletionInfo().isLive());
    }

    private void assertRepairContainsColumn(Mutation mutation,
                                            String clustering,
                                            String columnName,
                                            String value,
                                            long timestamp)
    {
        PartitionUpdate update = mutation.getPartitionUpdates().iterator().next();
        Row row = update.getRow(update.metadata().comparator.make(clustering));
        assertNotNull(row);
        assertColumn(cfm, row, columnName, value, timestamp);
    }

    private void assertRepairContainsNoColumns(Mutation mutation)
    {
        PartitionUpdate update = mutation.getPartitionUpdates().iterator().next();
        assertFalse(update.iterator().hasNext());
    }

    private void assertRepairMetadata(Mutation mutation)
    {
        PartitionUpdate update = mutation.getPartitionUpdates().iterator().next();
        assertEquals(update.metadata().keyspace, ks.getName());
        assertEquals(update.metadata().name, cfm.name);
    }

    private ReplicaPlan.SharedForRangeRead plan(EndpointsForRange replicas, ConsistencyLevel consistencyLevel)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForRangeRead(ks, ks.getReplicationStrategy(), consistencyLevel, ReplicaUtils.FULL_BOUNDS, replicas, replicas, 1));
    }

    private static void resolveAndConsume(DataResolver resolver)
    {
        try (PartitionIterator iterator = resolver.resolve())
        {
            while (iterator.hasNext())
            {
                try (RowIterator partition = iterator.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
        }
    }
}
