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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.ReplicaPlans.forReadRepair;
import static org.apache.cassandra.locator.ReplicaUtils.FULL_RANGE;
import static org.apache.cassandra.net.Verb.INTERNAL_RSP;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

@Ignore
public abstract  class AbstractReadRepairTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static TableMetadata cfm;
    static InetAddressAndPort target1;
    static InetAddressAndPort target2;
    static InetAddressAndPort target3;
    static InetAddressAndPort remote1;
    static InetAddressAndPort remote2;
    static InetAddressAndPort remote3;
    static List<InetAddressAndPort> targets;
    static List<InetAddressAndPort> remotes;

    static Replica replica1;
    static Replica replica2;
    static Replica replica3;
    static EndpointsForRange replicas;

    static Replica remoteReplica1;
    static Replica remoteReplica2;
    static Replica remoteReplica3;
    static EndpointsForRange remoteReplicas;


    static long now = TimeUnit.NANOSECONDS.toMicros(nanoTime());
    static DecoratedKey key;
    static Cell<?> cell1;
    static Cell<?> cell2;
    static Cell<?> cell3;
    static Mutation resolved;

    static ReadCommand command;

    static void assertRowsEqual(Row expected, Row actual)
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

    static void assertRowsEqual(RowIterator expected, RowIterator actual)
    {
        assertRowsEqual(expected.staticRow(), actual.staticRow());
        while (expected.hasNext())
        {
            assert actual.hasNext();
            assertRowsEqual(expected.next(), actual.next());
        }
        assert !actual.hasNext();
    }

    static void assertPartitionsEqual(PartitionIterator expected, PartitionIterator actual)
    {
        while (expected.hasNext())
        {
            assert actual.hasNext();
            assertRowsEqual(expected.next(), actual.next());
        }

        assert !actual.hasNext();
    }

    static void assertMutationEqual(Mutation expected, Mutation actual)
    {
        Assert.assertEquals(expected.getKeyspaceName(), actual.getKeyspaceName());
        Assert.assertEquals(expected.key(), actual.key());
        Assert.assertEquals(expected.key(), actual.key());
        PartitionUpdate expectedUpdate = Iterables.getOnlyElement(expected.getPartitionUpdates());
        PartitionUpdate actualUpdate = Iterables.getOnlyElement(actual.getPartitionUpdates());
        assertRowsEqual(Iterables.getOnlyElement(expectedUpdate), Iterables.getOnlyElement(actualUpdate));
    }

    static DecoratedKey dk(int v)
    {
        return DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(v));
    }

    static Cell<?> cell(String name, String value, long timestamp)
    {
        return BufferCell.live(cfm.getColumn(ColumnIdentifier.getInterned(name, false)), timestamp, ByteBufferUtil.bytes(value));
    }

    static PartitionUpdate update(Cell<?>... cells)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        for (Cell<?> cell: cells)
        {
            builder.addCell(cell);
        }
        return PartitionUpdate.singleRowUpdate(cfm, key, builder.build());
    }

    static PartitionIterator partition(Cell<?>... cells)
    {
        UnfilteredPartitionIterator iter = new SingletonUnfilteredPartitionIterator(update(cells).unfilteredIterator());
        return UnfilteredPartitionIterators.filter(iter, Ints.checkedCast(TimeUnit.MICROSECONDS.toSeconds(now)));
    }

    static Mutation mutation(Cell<?>... cells)
    {
        return new Mutation(update(cells));
    }

    static Message<ReadResponse> msg(InetAddressAndPort from, Cell<?>... cells)
    {
        UnfilteredPartitionIterator iter = new SingletonUnfilteredPartitionIterator(update(cells).unfilteredIterator());
        return Message.builder(INTERNAL_RSP, ReadResponse.createDataResponse(iter, command, command.executionController().getRepairedDataInfo()))
                      .from(from)
                      .build();
    }

    static class ResultConsumer implements Consumer<PartitionIterator>
    {

        PartitionIterator result = null;

        @Override
        public void accept(PartitionIterator partitionIterator)
        {
            Assert.assertNotNull(partitionIterator);
            result = partitionIterator;
        }
    }

    private static boolean configured = false;

    static void configureClass(ReadRepairStrategy repairStrategy) throws Throwable
    {
        ServerTestUtils.prepareServerNoRegister();

        target1 = InetAddressAndPort.getByName("127.1.0.255");
        target2 = InetAddressAndPort.getByName("127.1.0.254");
        target3 = InetAddressAndPort.getByName("127.1.0.253");

        remote1 = InetAddressAndPort.getByName("127.2.0.255");
        remote2 = InetAddressAndPort.getByName("127.2.0.254");
        remote3 = InetAddressAndPort.getByName("127.2.0.253");

        targets = ImmutableList.of(target1, target2, target3);
        remotes = ImmutableList.of(remote1, remote2, remote3);

        replica1 = fullReplica(target1, FULL_RANGE);
        replica2 = fullReplica(target2, FULL_RANGE);
        replica3 = fullReplica(target3, FULL_RANGE);
        replicas = EndpointsForRange.of(replica1, replica2, replica3);

        remoteReplica1 = fullReplica(remote1, FULL_RANGE);
        remoteReplica2 = fullReplica(remote2, FULL_RANGE);
        remoteReplica3 = fullReplica(remote3, FULL_RANGE);
        remoteReplicas = EndpointsForRange.of(remoteReplica1, remoteReplica2, remoteReplica3);

        String dataCenter1 = "datacenter1";
        String dataCenter2 = "datacenter2";
        String rack = "rack1";
        ClusterMetadataTestHelper.addEndpoint(replica1.endpoint(), new BytesToken(new byte[] { 0 }), dataCenter1, rack);
        ClusterMetadataTestHelper.addEndpoint(replica2.endpoint(), new BytesToken(new byte[] { 1 }), dataCenter1, rack);
        ClusterMetadataTestHelper.addEndpoint(replica3.endpoint(), new BytesToken(new byte[] { 2 }), dataCenter1, rack);
        ClusterMetadataTestHelper.addEndpoint(remoteReplica1.endpoint(), new BytesToken(new byte[] { 0 }), dataCenter2, rack);
        ClusterMetadataTestHelper.addEndpoint(remoteReplica2.endpoint(), new BytesToken(new byte[] { 1 }), dataCenter2, rack);
        ClusterMetadataTestHelper.addEndpoint(remoteReplica3.endpoint(), new BytesToken(new byte[] { 2 }), dataCenter2, rack);

        for (Replica replica : replicas)
        {
            UUID hostId = UUID.randomUUID();
            Gossiper.instance.initializeNodeUnsafe(replica.endpoint(), hostId, 1);
        }
        for (Replica replica : remoteReplicas)
        {
            UUID hostId = UUID.randomUUID();
            Gossiper.instance.initializeNodeUnsafe(replica.endpoint(), hostId, 1);
        }

        String ksName = "ks";

        String ddl = String.format("CREATE TABLE tbl (k int primary key, v text) WITH read_repair='%s'",
                                   toLowerCaseLocalized(repairStrategy.toString()));

        cfm = CreateTableStatement.parse(ddl, ksName).build();
        assert cfm.params.readRepair == repairStrategy;

        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), Tables.of(cfm));
        SchemaTestUtil.announceNewKeyspace(ksm);

        ks = Keyspace.open(ksName);
        cfs = ks.getColumnFamilyStore("tbl");

        cfs.sampleReadLatencyMicros = 0;
        cfs.additionalWriteLatencyMicros = 0;

        // default test values
        key  = dk(5);
        cell1 = cell("v", "val1", now);
        cell2 = cell("v", "val2", now);
        cell3 = cell("v", "val3", now);
        resolved = mutation(cell1, cell2);

        command = Util.cmd(cfs, 1).build();

        configured = true;
    }

    static Set<InetAddressAndPort> epSet(InetAddressAndPort... eps)
    {
        return Sets.newHashSet(eps);
    }

    @Before
    public void setUp()
    {
        assert configured : "configureClass must be called in a @BeforeClass method";

        cfs.sampleReadLatencyMicros = 0;
        cfs.additionalWriteLatencyMicros = 0;
    }

    static ReplicaPlan.ForRangeRead replicaPlan(ConsistencyLevel consistencyLevel, EndpointsForRange replicas)
    {
        return replicaPlan(ks, consistencyLevel, replicas, replicas);
    }

    static ReplicaPlan.ForWrite repairPlan(ReplicaPlan.ForRangeRead readPlan)
    {
        return repairPlan(readPlan, readPlan.readCandidates());
    }

    static ReplicaPlan.ForWrite repairPlan(EndpointsForRange liveAndDown, EndpointsForRange targets)
    {
        return repairPlan(replicaPlan(liveAndDown, targets), liveAndDown);
    }

    static ReplicaPlan.ForWrite repairPlan(ReplicaPlan.ForRangeRead readPlan, EndpointsForRange liveAndDown)
    {
        Token token = readPlan.range().left.getToken();
        EndpointsForToken pending = EndpointsForToken.empty(token);
        EndpointsForToken live = liveAndDown.forToken(token);
        return new ReplicaPlan.ForWrite(readPlan.keyspace(),
                                        readPlan.replicationStrategy(),
                                        readPlan.consistencyLevel(),
                                        pending,
                                        live,
                                        live,
                                        readPlan.contacts().forToken(token),
                                        (cm) -> null,
                                        Epoch.EMPTY);
    }
    static ReplicaPlan.ForRangeRead replicaPlan(EndpointsForRange replicas, EndpointsForRange targets)
    {
        return replicaPlan(ks, ConsistencyLevel.LOCAL_QUORUM, replicas, targets);
    }
    static ReplicaPlan.ForRangeRead replicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForRange replicas)
    {
        return replicaPlan(keyspace, consistencyLevel, replicas, replicas);
    }
    static ReplicaPlan.ForRangeRead replicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForRange replicas, EndpointsForRange targets)
    {
        return new ReplicaPlan.ForRangeRead(keyspace,
                                            keyspace.getReplicationStrategy(),
                                            consistencyLevel,
                                            ReplicaUtils.FULL_BOUNDS,
                                            replicas,
                                            targets,
                                            1,
                                            null,
                                            (self, token) -> forReadRepair(self, ClusterMetadata.current(), keyspace, consistencyLevel, token, (r) -> true),
                                            Epoch.EMPTY);
    }

    public abstract InstrumentedReadRepair createInstrumentedReadRepair(ReadCommand command, ReplicaPlan.Shared<?, ?> replicaPlan, Dispatcher.RequestTime requestTime);

    public InstrumentedReadRepair createInstrumentedReadRepair(ReplicaPlan.Shared<?, ?> replicaPlan)
    {
        return createInstrumentedReadRepair(command, replicaPlan, Dispatcher.RequestTime.forImmediateExecution());

    }

    /**
     * If we haven't received enough full data responses by the time the speculation
     * timeout occurs, we should send read requests to additional replicas
     */
    @Test
    public void readSpeculationCycle()
    {
        InstrumentedReadRepair repair = createInstrumentedReadRepair(ReplicaPlan.shared(replicaPlan(replicas, EndpointsForRange.of(replica1, replica2))));
        ResultConsumer consumer = new ResultConsumer();

        Assert.assertEquals(epSet(), repair.getReadRecipients());
        repair.startRepair(null, consumer);

        Assert.assertEquals(epSet(target1, target2), repair.getReadRecipients());
        repair.maybeSendAdditionalReads();
        Assert.assertEquals(epSet(target1, target2, target3), repair.getReadRecipients());
        Assert.assertNull(consumer.result);
    }

    /**
     * If we receive enough data responses by the before the speculation timeout
     * passes, we shouldn't send additional read requests
     */
    @Test
    public void noSpeculationRequired()
    {
        InstrumentedReadRepair repair = createInstrumentedReadRepair(ReplicaPlan.shared(replicaPlan(replicas, EndpointsForRange.of(replica1, replica2))));
        ResultConsumer consumer = new ResultConsumer();

        Assert.assertEquals(epSet(), repair.getReadRecipients());
        repair.startRepair(null, consumer);

        Assert.assertEquals(epSet(target1, target2), repair.getReadRecipients());
        repair.getReadCallback().onResponse(msg(target1, cell1));
        repair.getReadCallback().onResponse(msg(target2, cell1));

        repair.maybeSendAdditionalReads();
        Assert.assertEquals(epSet(target1, target2), repair.getReadRecipients());

        repair.awaitReads();

        assertPartitionsEqual(partition(cell1), consumer.result);
    }
}
