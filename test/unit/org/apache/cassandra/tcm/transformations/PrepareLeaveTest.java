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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.PlacementTransitionPlan;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.addr;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PrepareLeaveTest
{
    private static final Map<InetAddress, String> hostDc = new HashMap<>();
    private static KeyspaceMetadata KSM, KSM_NTS;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        for (int i = 1; i <= 10; i++)
            hostDc.put(InetAddress.getByName("127.0.0."+i), "dc1");
        for (int i = 11; i <= 20; i++)
            hostDc.put(InetAddress.getByName("127.0.0."+i), "dc2");

        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();

        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                String ep = endpoint.toString(false);
                return "rack"+ep.substring(ep.lastIndexOf('.') + 1);
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return hostDc.get(endpoint.getAddress());
            }
        });

        KSM = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3));
        KSM_NTS = KeyspaceMetadata.create("ks_nts", KeyspaceParams.nts("dc1", 3, "dc2", 3));
    }

    @Test
    public void testCheckRF_Simple() throws UnknownHostException
    {
        Keyspaces kss = Keyspaces.NONE.with(KSM);
        ClusterMetadata metadata = prepMetadata(kss, 2, 2);
        assertTrue(executeLeave(metadata));
        // should be rejected:
        metadata = prepMetadata(kss, 1, 2);
        assertFalse(executeLeave(metadata));
    }

    @Test
    public void testCheckRF_NTS() throws UnknownHostException
    {
        Keyspaces kss = Keyspaces.NONE.with(KSM_NTS);
        ClusterMetadata metadata = prepMetadata(kss, 4, 4);
        assertTrue(executeLeave(metadata));
        // should be accepted (4 nodes in dc1 where we remove the host):
        metadata = prepMetadata(kss, 4, 2);
        assertTrue(executeLeave(metadata));
        // should be rejected
        metadata = prepMetadata(kss, 3, 4);
        assertFalse(executeLeave(metadata));
    }

    private boolean executeLeave(ClusterMetadata metadata) throws UnknownHostException
    {
        PrepareLeave prepareLeave = new PrepareLeave(metadata.directory.peerId(InetAddressAndPort.getByName("127.0.0.1")),
                                                     false,
                                                     dummyPlacementProvider);

        return prepareLeave.execute(metadata).isSuccess();
    }

    private ClusterMetadata prepMetadata(Keyspaces kss, int countDc1, int countDc2) throws UnknownHostException
    {
        DistributedSchema schema = new DistributedSchema(kss);
        Directory dir = new Directory();
        for (int i = 1; i <= countDc1; i++)
        {
            InetAddressAndPort ep = InetAddressAndPort.getByName("127.0.0."+i);
            Location l = new Location(hostDc.get(ep.getAddress()), "rack" + i);
            dir = dir.with(addr(ep), l);
            dir = dir.withNodeState(dir.peerId(ep), NodeState.JOINED);
        }
        for (int i = 11; i <= 10 + countDc2; i++)
        {
            InetAddressAndPort ep = InetAddressAndPort.getByName("127.0.0."+i);
            Location l = new Location(hostDc.get(ep.getAddress()), "rack"+i);
            dir = dir.with(addr(ep), l);
            dir = dir.withNodeState(dir.peerId(ep), NodeState.JOINED);
        }
        ClusterMetadata.Transformer transformer = new ClusterMetadata(Murmur3Partitioner.instance,
                                                                      dir,
                                                                      schema).transformer();
        Random r = new Random(System.nanoTime());
        Set<Token> tokens = new HashSet<>(dir.peerIds().size());
        while(tokens.size() < dir.peerIds().size())
            tokens.add(Murmur3Partitioner.instance.getRandomToken(r));
        Iterator<Token> iter = tokens.iterator();
        for (NodeId node : dir.peerIds())
            transformer = transformer.proposeToken(node, Collections.singleton(iter.next()));

        return transformer.build().metadata;
    }

    public static PlacementProvider dummyPlacementProvider = new PlacementProvider()
    {
        @Override
        public DataPlacements calculatePlacements(List<Range<Token>> ranges, ClusterMetadata metadata, Keyspaces keyspaces) { return null; }

        @Override
        public PlacementTransitionPlan planForJoin(ClusterMetadata metadata, NodeId nodeId, Set<Token> tokens, Keyspaces keyspaces) { return null;}

        @Override
        public PlacementTransitionPlan planForMove(ClusterMetadata metadata, NodeId nodeId, Set<Token> tokens, Keyspaces keyspaces)
        {
            return null;
        }

        @Override
        public PlacementTransitionPlan planForDecommission(ClusterMetadata metadata, NodeId nodeId, Keyspaces keyspaces)
        {
            return new PlacementTransitionPlan(PlacementDeltas.empty(),
                                               PlacementDeltas.empty(),
                                               PlacementDeltas.empty(),
                                               PlacementDeltas.empty());
        }

        @Override
        public PlacementTransitionPlan planForReplacement(ClusterMetadata metadata, NodeId replaced, NodeId replacement, Keyspaces keyspaces) { return null; }
    };
}
