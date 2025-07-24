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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Journal;
import accord.impl.AbstractConfigurationServiceTest;
import accord.local.Node.Id;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncResult;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.journal.AccordTopologyUpdate;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ValidatingClusterMetadataService;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.MockFailureDetector;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.impl.AbstractConfigurationServiceTest.TestListener;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

public class AccordConfigurationServiceTest
{
    private static final Id ID1 = new Id(1);
    private static final Id ID2 = new Id(2);
    private static final Id ID3 = new Id(3);
    private static final SortedArrayList<Id> ID_LIST = new SortedArrayList<>(new Id[] { ID1, ID2, ID3 });
    private static final String KEYSPACE_NAME = "test_ks";
    private static final TableId TBL_ID = TableId.fromUUID(new UUID(0, 1));

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

        @Override
        public <V> void respond(V response, Message<?> message)
        {
            throw new UnsupportedOperationException();
        }
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
    }

    @Test
    public void loadTest() throws Throwable
    {
        ValidatingClusterMetadataService cms = ValidatingClusterMetadataService.createAndRegister(Version.MIN_ACCORD_VERSION);

        AccordJournal journal = null;
        try
        {
            journal = initJournal();
            AccordConfigurationService service = new AccordConfigurationService(ID1, new AccordAgent(), new Messaging(), new MockFailureDetector(), ScheduledExecutors.scheduledTasks);
            AccordJournal journal_ = journal;
            TestListener listener = new TestListener(service, true)
            {
                @Override
                public AsyncResult<Void> onTopologyUpdate(Topology topology, boolean isLoad, boolean startSync)
                {
                    // Fake journal save
                    journal_.saveTopology(new Journal.TopologyUpdate(new Int2ObjectHashMap<>(), topology), () -> {});
                    return super.onTopologyUpdate(topology, isLoad, startSync);
                }
            };
            service.registerListener(listener);
            service.start();

            Topology topology1 = createTopology(cms);
            service.updateMapping(mappingForEpoch(cms.metadata().epoch.getEpoch() + 1));
            service.reportTopology(topology1);
            service.receiveRemoteSyncComplete(ID1, 1);
            service.receiveRemoteSyncComplete(ID2, 1);
            service.receiveRemoteSyncComplete(ID3, 1);

            Topology topology2 = createTopology(cms);
            service.reportTopology(topology2);
            service.receiveRemoteSyncComplete(ID1, 2);

            Topology topology3 = createTopology(cms);
            service.reportTopology(topology3);

            AccordConfigurationService loaded = new AccordConfigurationService(ID1, new AccordAgent(), new Messaging(), new MockFailureDetector(), ScheduledExecutors.scheduledTasks);
            loaded.updateMapping(mappingForEpoch(cms.metadata().epoch.getEpoch() + 1));
            listener = new AbstractConfigurationServiceTest.TestListener(loaded, true);
            loaded.registerListener(listener);
            Iterator<AccordTopologyUpdate.ImmutableTopoloyImage> iter = journal.replayTopologies();
            // Simulate journal replay
            while (iter.hasNext())
                loaded.reportTopology(iter.next().global);
            loaded.start();

            listener.assertTopologiesFor(1L, 2L, 3L);
            listener.assertTopologyForEpoch(1, topology1);
            listener.assertTopologyForEpoch(2, topology2);
            listener.assertTopologyForEpoch(3, topology3);
        }
        finally
        {
            if (journal != null)
                journal.shutdown();
        }
    }

    private static AccordJournal initJournal() throws Throwable
    {
        File directory = new File(Files.createTempDirectory("config_service_test"));
        directory.deleteRecursiveOnExit();
        Keyspace ks = Schema.instance.getKeyspaceInstance("system_accord");
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(AccordKeyspace.JOURNAL);
        AccordJournal journal = new AccordJournal(new TestParams(), directory, cfs);
        journal.start(null);
        journal.unsafeSetStarted();
        return journal;
    }
    private static Topology createTopology(ValidatingClusterMetadataService cms)
    {
        ClusterMetadata previous = cms.metadata();
        ClusterMetadata.Transformer next = previous.transformer();
        maybeCreateTable(previous, next);

        ClusterMetadata metadata = next.build().metadata;
        cms.setMetadata(metadata);
        return AccordTopology.createAccordTopology(metadata);
    }

    private static void maybeCreateTable(ClusterMetadata previous, ClusterMetadata.Transformer next)
    {
        Optional<KeyspaceMetadata> ks = previous.schema.getKeyspaces().get(KEYSPACE_NAME);
        if (ks.isPresent()) return;
        // lets create it
        TableMetadata table = TableMetadata.builder(KEYSPACE_NAME, "tbl")
                .id(TBL_ID)
                .kind(TableMetadata.Kind.REGULAR)
                .partitioner(Murmur3Partitioner.instance)
                .addPartitionKeyColumn("pk", Int32Type.instance)
                .build();
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(KEYSPACE_NAME, KeyspaceParams.simple(ID_LIST.size()))
                .withSwapped(Tables.builder().add(table).build());

        next.with(new DistributedSchema(previous.schema.getKeyspaces().with(keyspace)));

        for (Id node : ID_LIST)
        {
            // not forcing the cms node id to match as they do when this logic was first added...
            next.register(new NodeAddresses(getAddress(node)),
                    new Location("dc1", "rack1"),
                    NodeVersion.CURRENT);

            next.proposeToken(new NodeId(node.id), Collections.singleton(new Murmur3Partitioner.LongToken(node.id)));
        }

        DataPlacement.Builder replication = DataPlacement.builder();
        Range<Token> fullRange = new Range<>(Murmur3Partitioner.MINIMUM, Murmur3Partitioner.MINIMUM);
        for (int i = 0; i < ID_LIST.size(); i++)
        {
            InetAddressAndPort address = getAddress(ID_LIST.get(i));
            Replica replica = new Replica(address, fullRange, true);
            replication.withReadReplica(next.epoch(), replica).withWriteReplica(next.epoch(), replica);
        }
        next.with(previous.placements.unbuild().with(keyspace.params.replication, replication.build()).build());
    }

    private static InetAddressAndPort getAddress(Id node)
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[]{127, 0, 0, (byte) node.id});
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
