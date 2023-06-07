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

package org.apache.cassandra.service.accord;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.ConfigurationService.EpochReady;
import accord.impl.AbstractConfigurationServiceTest;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Shard;
import accord.topology.Topology;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace.EpochDiskState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.MockFailureDetector;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.impl.AbstractConfigurationServiceTest.TestListener;
import static com.google.common.collect.ImmutableSet.of;
import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.AccordKeyspace.EPOCH_METADATA;
import static org.apache.cassandra.service.accord.AccordKeyspace.TOPOLOGIES;
import static org.apache.cassandra.service.accord.AccordKeyspace.loadEpoch;

public class AccordConfigurationServiceTest
{
    private static final Id ID1 = new Id(1);
    private static final Id ID2 = new Id(2);
    private static final Id ID3 = new Id(3);
    private static final List<Id> ID_LIST = ImmutableList.of(ID1, ID2, ID3);
    private static final Set<Id> ID_SET = ImmutableSet.copyOf(ID_LIST);
    private static final TableId TBL1 = TableId.fromUUID(new UUID(0, 1));
    private static final TableId TBL2 = TableId.fromUUID(new UUID(0, 2));

    private static EndpointMapping mappingForEpoch(long epoch)
    {
        try
        {
            EndpointMapping.Builder builder = EndpointMapping.builder(epoch);
            builder.add(InetAddressAndPort.getByName("127.0.0.1"), ID1);
            builder.add(InetAddressAndPort.getByName("127.0.0.2"), ID2);
            builder.add(InetAddressAndPort.getByName("127.0.0.3"), ID3);
            return builder.build();
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static EndpointMapping mappingForTopology(Topology topology)
    {
        try
        {
            EndpointMapping.Builder builder = EndpointMapping.builder(topology.epoch());
            for (Node.Id id : topology.nodes())
                builder.add(InetAddressAndPort.getByName("127.0.0." + id.id), id);
            return builder.build();
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class Messaging implements MessageDelivery
    {
        static class Request
        {
            final Message<?> message;
            final InetAddressAndPort to;
            final RequestCallback<?> callback;

            public Request(Message<?> message, InetAddressAndPort to, RequestCallback<?> callback)
            {
                this.message = message;
                this.to = to;
                this.callback = callback;
            }
        }

        final List<Request> requests = new ArrayList<>();

        @Override
        public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
        {
            requests.add(new Request(message, to, null));
        }

        @Override
        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
        {
            requests.add(new Request(message, to, cb));
        }

        @Override
        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
        {
            throw new UnsupportedOperationException();
        }
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
    }

    @Before
    public void setup()
    {
        Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStore(TOPOLOGIES).truncateBlocking();
        Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStore(EPOCH_METADATA).truncateBlocking();
    }

    @Test
    public void initialEpochTest() throws Throwable
    {
        AccordConfigurationService service = new AccordConfigurationService(ID1, new Messaging(), new MockFailureDetector());
        Assert.assertEquals(null, AccordKeyspace.loadEpochDiskState());
        service.start();
        Assert.assertEquals(null, AccordKeyspace.loadEpochDiskState());
        Assert.assertTrue(executeInternal(format("SELECT * FROM %s.%s WHERE epoch=1", ACCORD_KEYSPACE_NAME, TOPOLOGIES)).isEmpty());

        Topology topology1 = new Topology(1, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, ID_SET));
        service.reportTopology(topology1);
        loadEpoch(1, (epoch, topology, syncStatus, pendingSync, remoteSync) -> {
            Assert.assertEquals(topology1, topology);
            Assert.assertTrue(remoteSync.isEmpty());
        });
        Assert.assertEquals(new EpochDiskState(1, 1), service.diskState());

        service.remoteSyncComplete(ID1, 1);
        service.remoteSyncComplete(ID2, 1);
        loadEpoch(1, (epoch, topology, syncStatus, pendingSync, remoteSync) -> {
            Assert.assertEquals(topology1, topology);
            Assert.assertEquals(Sets.newHashSet(ID1, ID2), remoteSync);
        });
    }

    @Test
    public void loadTest() throws Throwable
    {
        AccordConfigurationService service = new AccordConfigurationService(ID1, new Messaging(), new MockFailureDetector());
        service.start();

        Topology topology1 = new Topology(1, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, ID_SET));
        service.updateMapping(mappingForEpoch(ClusterMetadata.current().epoch.getEpoch() + 1));
        service.reportTopology(topology1);
        service.acknowledgeEpoch(EpochReady.done(1));
        service.remoteSyncComplete(ID1, 1);
        service.remoteSyncComplete(ID2, 1);
        service.remoteSyncComplete(ID3, 1);

        Topology topology2 = new Topology(2, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, of(ID1, ID2)));
        service.reportTopology(topology2);
        service.acknowledgeEpoch(EpochReady.done(2));
        service.remoteSyncComplete(ID1, 2);

        Topology topology3 = new Topology(3, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, of(ID1, ID2)));
        service.reportTopology(topology3);
        service.acknowledgeEpoch(EpochReady.done(3));

        AccordConfigurationService loaded = new AccordConfigurationService(ID1, new Messaging(), new MockFailureDetector());
        loaded.updateMapping(mappingForEpoch(ClusterMetadata.current().epoch.getEpoch() + 1));
        AbstractConfigurationServiceTest.TestListener listener = new AbstractConfigurationServiceTest.TestListener(loaded, true);
        loaded.registerListener(listener);
        loaded.start();

        listener.assertNoTruncates();
        listener.assertTopologiesFor(1L, 2L, 3L);
        listener.assertTopologyForEpoch(1, topology1);
        listener.assertTopologyForEpoch(2, topology2);
        listener.assertTopologyForEpoch(3, topology3);
        listener.assertSyncsFor(1L, 2L);
        listener.assertSyncsForEpoch(1, ID1, ID2, ID3);
        listener.assertSyncsForEpoch(2, ID1);
    }

    @Test
    public void truncateTest()
    {
        AccordConfigurationService service = new AccordConfigurationService(ID1, new Messaging(), new MockFailureDetector());
        TestListener serviceListener = new TestListener(service, true);
        service.registerListener(serviceListener);
        service.start();

        Topology topology1 = new Topology(1, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, ID_SET));
        service.updateMapping(mappingForEpoch(ClusterMetadata.current().epoch.getEpoch() + 1));
        service.reportTopology(topology1);

        Topology topology2 = new Topology(2, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, of(ID1, ID2)));
        service.reportTopology(topology2);

        Topology topology3 = new Topology(3, new Shard(AccordTopologyUtils.fullRange("ks"), ID_LIST, of(ID1, ID2)));
        service.reportTopology(topology3);
        service.truncateTopologiesUntil(3);
        Assert.assertEquals(new EpochDiskState(3, 3), service.diskState());
        serviceListener.assertTruncates(3L);

        AccordConfigurationService loaded = new AccordConfigurationService(ID1, new Messaging(), new MockFailureDetector());
        loaded.updateMapping(mappingForEpoch(ClusterMetadata.current().epoch.getEpoch() + 1));
        TestListener loadListener = new TestListener(loaded, true);
        loaded.registerListener(loadListener);
        loaded.start();
        loadListener.assertTopologiesFor(3L);
    }
}
