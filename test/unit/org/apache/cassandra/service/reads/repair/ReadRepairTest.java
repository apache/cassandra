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

package org.apache.cassandra.service.reads.repair;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ReadRepairTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static TableMetadata cfm;
    static InetAddressAndPort target1;
    static InetAddressAndPort target2;
    static InetAddressAndPort target3;
    static List<InetAddressAndPort> targets;

    private static class InstrumentedReadRepairHandler extends BlockingPartitionRepair
    {
        public InstrumentedReadRepairHandler(Keyspace keyspace, DecoratedKey key, ConsistencyLevel consistency, Map<InetAddressAndPort, Mutation> repairs, int maxBlockFor, InetAddressAndPort[] participants)
        {
            super(keyspace, key, consistency, repairs, maxBlockFor, participants);
        }

        Map<InetAddressAndPort, Mutation> mutationsSent = new HashMap<>();

        protected void sendRR(MessageOut<Mutation> message, InetAddressAndPort endpoint)
        {
            mutationsSent.put(endpoint, message.payload);
        }

        List<InetAddressAndPort> candidates = targets;

        protected List<InetAddressAndPort> getCandidateEndpoints()
        {
            return candidates;
        }

        @Override
        protected boolean isLocal(InetAddressAndPort endpoint)
        {
            return targets.contains(endpoint);
        }
    }

    static long now = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
    static DecoratedKey key;
    static Cell cell1;
    static Cell cell2;
    static Cell cell3;
    static Mutation resolved;

    private static void assertRowsEqual(Row expected, Row actual)
    {
        try
        {
            Assert.assertEquals(expected == null, actual == null);
            if (expected == null)
                return;
            Assert.assertEquals(expected.clustering(), actual.clustering());
            Assert.assertEquals(expected.deletion(), actual.deletion());
            Assert.assertArrayEquals(Iterables.toArray(expected.cells(), Cell.class), Iterables.toArray(expected.cells(), Cell.class));
        } catch (Throwable t)
        {
            throw new AssertionError(String.format("Row comparison failed, expected %s got %s", expected, actual), t);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        String ksName = "ks";

        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k int primary key, v text)", ksName).build();
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(3), Tables.of(cfm));
        MigrationManager.announceNewKeyspace(ksm, false);

        ks = Keyspace.open(ksName);
        cfs = ks.getColumnFamilyStore("tbl");

        cfs.sampleLatencyNanos = 0;

        target1 = InetAddressAndPort.getByName("127.0.0.255");
        target2 = InetAddressAndPort.getByName("127.0.0.254");
        target3 = InetAddressAndPort.getByName("127.0.0.253");

        targets = ImmutableList.of(target1, target2, target3);

        // default test values
        key  = dk(5);
        cell1 = cell("v", "val1", now);
        cell2 = cell("v", "val2", now);
        cell3 = cell("v", "val3", now);
        resolved = mutation(cell1, cell2);
    }

    private static DecoratedKey dk(int v)
    {
        return DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(v));
    }

    private static Cell cell(String name, String value, long timestamp)
    {
        return BufferCell.live(cfm.getColumn(ColumnIdentifier.getInterned(name, false)), timestamp, ByteBufferUtil.bytes(value));
    }

    private static Mutation mutation(Cell... cells)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder(0);
        builder.newRow(Clustering.EMPTY);
        for (Cell cell: cells)
        {
            builder.addCell(cell);
        }
        return new Mutation(PartitionUpdate.singleRowUpdate(cfm, key, builder.build()));
    }

    private static InstrumentedReadRepairHandler createRepairHandler(Map<InetAddressAndPort, Mutation> repairs, int maxBlockFor, Collection<InetAddressAndPort> participants)
    {
        InetAddressAndPort[] participantArray = new InetAddressAndPort[participants.size()];
        participants.toArray(participantArray);
        return new InstrumentedReadRepairHandler(ks, key, ConsistencyLevel.LOCAL_QUORUM, repairs, maxBlockFor, participantArray);
    }

    private static InstrumentedReadRepairHandler createRepairHandler(Map<InetAddressAndPort, Mutation> repairs, int maxBlockFor)
    {
        return createRepairHandler(repairs, maxBlockFor, repairs.keySet());
    }

    @Test
    public void consistencyLevelTest() throws Exception
    {
        Assert.assertTrue(ConsistencyLevel.QUORUM.satisfies(ConsistencyLevel.QUORUM, ks));
        Assert.assertTrue(ConsistencyLevel.THREE.satisfies(ConsistencyLevel.QUORUM, ks));
        Assert.assertTrue(ConsistencyLevel.TWO.satisfies(ConsistencyLevel.QUORUM, ks));
        Assert.assertFalse(ConsistencyLevel.ONE.satisfies(ConsistencyLevel.QUORUM, ks));
        Assert.assertFalse(ConsistencyLevel.ANY.satisfies(ConsistencyLevel.QUORUM, ks));
    }

    private static void assertMutationEqual(Mutation expected, Mutation actual)
    {
        Assert.assertEquals(expected.getKeyspaceName(), actual.getKeyspaceName());
        Assert.assertEquals(expected.key(), actual.key());
        Assert.assertEquals(expected.key(), actual.key());
        PartitionUpdate expectedUpdate = Iterables.getOnlyElement(expected.getPartitionUpdates());
        PartitionUpdate actualUpdate = Iterables.getOnlyElement(actual.getPartitionUpdates());
        assertRowsEqual(Iterables.getOnlyElement(expectedUpdate), Iterables.getOnlyElement(actualUpdate));
    }

    @Test
    public void additionalMutationRequired() throws Exception
    {

        Mutation repair1 = mutation(cell2);
        Mutation repair2 = mutation(cell1);

        // check that the correct repairs are calculated
        Map<InetAddressAndPort, Mutation> repairs = new HashMap<>();
        repairs.put(target1, repair1);
        repairs.put(target2, repair2);


        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, 2);

        Assert.assertTrue(handler.mutationsSent.isEmpty());

        // check that the correct mutations are sent
        handler.sendInitialRepairs();
        Assert.assertEquals(2, handler.mutationsSent.size());
        assertMutationEqual(repair1, handler.mutationsSent.get(target1));
        assertMutationEqual(repair2, handler.mutationsSent.get(target2));

        // check that a combined mutation is speculatively sent to the 3rd target
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertEquals(1, handler.mutationsSent.size());
        assertMutationEqual(resolved, handler.mutationsSent.get(target3));

        // check repairs stop blocking after receiving 2 acks
        Assert.assertFalse(handler.awaitRepairs(0, TimeUnit.NANOSECONDS));
        handler.ack(target1);
        Assert.assertFalse(handler.awaitRepairs(0, TimeUnit.NANOSECONDS));
        handler.ack(target3);
        Assert.assertTrue(handler.awaitRepairs(0, TimeUnit.NANOSECONDS));

    }

    /**
     * If we've received enough acks, we shouldn't send any additional mutations
     */
    @Test
    public void noAdditionalMutationRequired() throws Exception
    {
        Map<InetAddressAndPort, Mutation> repairs = new HashMap<>();
        repairs.put(target1, mutation(cell2));
        repairs.put(target2, mutation(cell1));

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, 2);
        handler.sendInitialRepairs();
        handler.ack(target1);
        handler.ack(target2);

        // both replicas have acked, we shouldn't send anything else out
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertTrue(handler.mutationsSent.isEmpty());
    }

    /**
     * If there are no additional nodes we can send mutations to, we... shouldn't
     */
    @Test
    public void noAdditionalMutationPossible() throws Exception
    {
        Map<InetAddressAndPort, Mutation> repairs = new HashMap<>();
        repairs.put(target1, mutation(cell2));
        repairs.put(target2, mutation(cell1));

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, 2);
        handler.sendInitialRepairs();

        // we've already sent mutations to all candidates, so we shouldn't send any more
        handler.candidates = Lists.newArrayList(target1, target2);
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertTrue(handler.mutationsSent.isEmpty());
    }

    /**
     * If we didn't send a repair to a replica because there wasn't a diff with the
     * resolved column family, we shouldn't send it a speculative mutation
     */
    @Test
    public void mutationsArentSentToInSyncNodes() throws Exception
    {
        Mutation repair1 = mutation(cell2);

        Map<InetAddressAndPort, Mutation> repairs = new HashMap<>();
        repairs.put(target1, repair1);
        Collection<InetAddressAndPort> participants = Lists.newArrayList(target1, target2);

        // check that the correct initial mutations are sent out
        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, 2, participants);
        handler.sendInitialRepairs();
        Assert.assertEquals(1, handler.mutationsSent.size());
        Assert.assertTrue(handler.mutationsSent.containsKey(target1));

        // check that speculative mutations aren't sent to target2
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertEquals(1, handler.mutationsSent.size());
        Assert.assertTrue(handler.mutationsSent.containsKey(target3));
    }

    @Test
    public void onlyBlockOnQuorum()
    {
        Map<InetAddressAndPort, Mutation> repairs = new HashMap<>();
        repairs.put(target1, mutation(cell1));
        repairs.put(target2, mutation(cell2));
        repairs.put(target3, mutation(cell3));
        Assert.assertEquals(3, repairs.size());

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, 2);
        handler.sendInitialRepairs();

        Assert.assertFalse(handler.awaitRepairs(0, TimeUnit.NANOSECONDS));
        handler.ack(target1);
        Assert.assertFalse(handler.awaitRepairs(0, TimeUnit.NANOSECONDS));

        // here we should stop blocking, even though we've sent 3 repairs
        handler.ack(target2);
        Assert.assertTrue(handler.awaitRepairs(0, TimeUnit.NANOSECONDS));

    }

    /**
     * For dc local consistency levels, noop mutations and responses from remote dcs should not affect effective blockFor
     */
    @Test
    public void remoteDCTest() throws Exception
    {
        Map<InetAddressAndPort, Mutation> repairs = new HashMap<>();
        repairs.put(target1, mutation(cell1));


        InetAddressAndPort remote1 = InetAddressAndPort.getByName("10.0.0.1");
        InetAddressAndPort remote2 = InetAddressAndPort.getByName("10.0.0.2");
        repairs.put(remote1, mutation(cell1));

        Collection<InetAddressAndPort> participants = Lists.newArrayList(target1, target2, remote1, remote2);

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, 2, participants);
        handler.sendInitialRepairs();
        Assert.assertEquals(2, handler.mutationsSent.size());
        Assert.assertTrue(handler.mutationsSent.containsKey(target1));
        Assert.assertTrue(handler.mutationsSent.containsKey(remote1));

        Assert.assertEquals(1, handler.waitingOn());
        Assert.assertFalse(handler.awaitRepairs(0, TimeUnit.NANOSECONDS));

        handler.ack(remote1);
        Assert.assertEquals(1, handler.waitingOn());
        Assert.assertFalse(handler.awaitRepairs(0, TimeUnit.NANOSECONDS));

        handler.ack(target1);
        Assert.assertEquals(0, handler.waitingOn());
        Assert.assertTrue(handler.awaitRepairs(0, TimeUnit.NANOSECONDS));
    }
}
