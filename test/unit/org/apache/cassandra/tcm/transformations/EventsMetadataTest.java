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

package org.apache.cassandra.tcm.transformations;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;

import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.service.LeaveAndBootstrapTest.bootstrapping;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// TODO: try to rewrite them as fuzz tests
public class EventsMetadataTest
{
    public static KeyspaceMetadata KSM, KSM_NTS;
    private static final InetAddressAndPort node1;
    private static final InetAddressAndPort node2;

    static
    {
        try
        {
            node1 = InetAddressAndPort.getByName("127.0.0.1");
            node2 = InetAddressAndPort.getByName("127.0.0.2");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void beforeClass()
    {
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServer();
        KSM = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3));
        KSM_NTS = KeyspaceMetadata.create("ks_nts", KeyspaceParams.nts("dc1", 3, "dc2", 3));
    }

    @Before
    public void before() throws ExecutionException, InterruptedException
    {
        ServerTestUtils.resetCMS();
    }

    @After
    public void after()
    {
        ClusterMetadataService.unsetInstance();
    }

    @Test
    public void firstRegisterTest()
    {
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KSM, Schema.instance);
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KSM_NTS, Schema.instance);

        ClusterMetadata metadata = ClusterMetadataService.instance().commit(register(node1));
        NodeId nodeId = metadata.directory.peerId(node1);
        // register adds to directory;
        assertEquals(node1, metadata.directory.endpoint(nodeId));

        // should not be in tokenMap (no tokens yet)
        assertTrue(metadata.tokenMap.tokens(nodeId).isEmpty());
        assertTrue(metadata.placements.get(KSM.params.replication).writes.byEndpoint().isEmpty());
        assertTrue(metadata.placements.get(KSM.params.replication).reads.byEndpoint().isEmpty());
        assertTrue(metadata.lockedRanges.locked.isEmpty());
    }

    @Test
    public void prepareJoinTest() throws ExecutionException, InterruptedException
    {
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KSM, Schema.instance);
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KSM_NTS, Schema.instance);

        ClusterMetadata metadata = ClusterMetadataService.instance().commit(register(node1));
        NodeId nodeId = metadata.directory.peerId(node1);

        metadata = ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareJoin(nodeId));

        assertFalse(metadata.tokenMap.tokens(nodeId).isEmpty());
        assertEquals(NodeState.REGISTERED, metadata.directory.peerState(nodeId));
    }

    @Test
    public void joinTest() throws ExecutionException, InterruptedException
    {
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KSM, Schema.instance);
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KSM_NTS, Schema.instance);

        ClusterMetadataService.instance().commit(register(node1));

        NodeId nodeId = ClusterMetadata.current().directory.peerId(node1);

        ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareJoin(nodeId));
        BootstrapAndJoin plan = (BootstrapAndJoin) ClusterMetadata.current().inProgressSequences.get(nodeId);

        ClusterMetadataService.instance().commit(plan.startJoin);

        assertFalse(ClusterMetadata.current().tokenMap.tokens(nodeId).isEmpty());
        assertEquals(NodeState.BOOTSTRAPPING, ClusterMetadata.current().directory.peerState(nodeId));

        assertTrue(ClusterMetadata.current().placements.get(KSM.params.replication).writes.byEndpoint().containsKey(node1));
        // the first joined node gets added to the read endpoints immediately
        assertTrue(ClusterMetadata.current().placements.get(KSM.params.replication).reads.byEndpoint().containsKey(node1));

        ClusterMetadataService.instance().commit(plan.midJoin);
        ClusterMetadataService.instance().commit(plan.finishJoin);

        assertEquals(NodeState.JOINED, ClusterMetadata.current().directory.peerState(nodeId));
        assertTrue(ClusterMetadata.current().lockedRanges.locked.isEmpty());
        assertTrue(bootstrapping(ClusterMetadata.current()).isEmpty());

        // join a second node
        ClusterMetadataService.instance().commit(register(node2));
        nodeId = ClusterMetadata.current().directory.peerId(node2);
        ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareJoin(nodeId));

        plan = (BootstrapAndJoin) ClusterMetadata.current().inProgressSequences.get(nodeId);

        ClusterMetadataService.instance().commit(plan.startJoin);

        assertFalse(ClusterMetadata.current().tokenMap.tokens(nodeId).isEmpty());
        assertEquals(NodeState.BOOTSTRAPPING, ClusterMetadata.current().directory.peerState(nodeId));
        assertTrue(ClusterMetadata.current().placements.get(KSM.params.replication).writes.byEndpoint().containsKey(node2));
        assertFalse(ClusterMetadata.current().placements.get(KSM.params.replication).reads.byEndpoint().containsKey(node2));
    }

    @Test
    public void leaveTest() throws ExecutionException, InterruptedException
    {
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KSM, Schema.instance);
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KSM_NTS, Schema.instance);

        ClusterMetadataService.instance().commit(register(node1));

        NodeId nodeId = ClusterMetadata.current().directory.peerId(node1);

        ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareJoin(nodeId));
        BootstrapAndJoin join = (BootstrapAndJoin) ClusterMetadata.current().inProgressSequences.get(nodeId);

        ClusterMetadataService.instance().commit(join.startJoin);
        ClusterMetadataService.instance().commit(join.midJoin);
        ClusterMetadataService.instance().commit(join.finishJoin);

        ClusterMetadata before = ClusterMetadata.current();
        ClusterMetadataService.instance().commit(new PrepareLeave(nodeId, true, PrepareLeaveTest.dummyPlacementProvider));
        UnbootstrapAndLeave leave = (UnbootstrapAndLeave) ClusterMetadata.current().inProgressSequences.get(nodeId);
        ClusterMetadata after = ClusterMetadata.current();
        // no change in metadata after prepareLeave;
        assertEquals(before.directory, after.directory);
        assertEquals(before.tokenMap, after.tokenMap);
        assertEquals(before.placements, after.placements);
        assertEquals(before.schema, after.schema);

        ClusterMetadataService.instance().commit(leave.startLeave);
        ClusterMetadataService.instance().commit(leave.midLeave);
        ClusterMetadataService.instance().commit(leave.finishLeave);

    }

    public static Register register(InetAddressAndPort endpoint)
    {
        return new Register(new NodeAddresses(endpoint, endpoint, endpoint), new Location("dc1", "rack1"), NodeVersion.CURRENT);
    }
}

