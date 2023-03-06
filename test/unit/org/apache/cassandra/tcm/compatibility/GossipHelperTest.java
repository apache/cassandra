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

package org.apache.cassandra.tcm.compatibility;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.PlacementForRange;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.gms.ApplicationState.*;
import static org.apache.cassandra.gms.VersionedValue.*;
import static org.apache.cassandra.locator.InetAddressAndPort.getByName;
import static org.apache.cassandra.tcm.compatibility.GossipHelper.fromEndpointStates;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GossipHelperTest
{
    private static KeyspaceMetadata KSM, KSM_NTS;
    private static final VersionedValue.VersionedValueFactory vvf = new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance);

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServerNoRegister();
        KSM = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3));
        KSM_NTS = KeyspaceMetadata.create("ks_nts", KeyspaceParams.nts("dc1", 3, "dc2", 3));
    }
    @Test
    public void singleInstanceFromGossipTest() throws UnknownHostException
    {
        Keyspaces kss = Keyspaces.NONE.with(KSM);
        DistributedSchema schema = new DistributedSchema(kss);
        Map<InetAddressAndPort, EndpointState> epstates = new HashMap<>();
        InetAddressAndPort endpoint = getByName("127.0.0.2"); // 127.0.0.1 is localhost, avoid that
        InetAddressAndPort internal = getByName("127.0.0.3");
        InetAddressAndPort nativeAddress = getByName("127.0.0.4");
        UUID hostId = UUID.randomUUID();
        Token token = t(12345);
        epstates.put(endpoint, epstate(internal, nativeAddress, token, hostId, "dc1"));
        ClusterMetadata metadata = fromEndpointStates(epstates, Murmur3Partitioner.instance, schema);
        NodeId nodeId = metadata.directory.peerId(endpoint);
        assertEquals(hostId, metadata.directory.hostId(nodeId));
        assertEquals(token, metadata.tokenMap.tokens(nodeId).iterator().next());
        assertEquals("dc1", metadata.directory.location(nodeId).datacenter);
        assertEquals("rack1", metadata.directory.location(nodeId).rack);
        assertEquals(Version.OLD, metadata.directory.versions.get(nodeId).serializationVersion);
        assertEquals(new CassandraVersion("3.0.24"), metadata.directory.versions.get(nodeId).cassandraVersion);
        assertEquals(internal, metadata.directory.addresses.get(nodeId).localAddress);
        assertEquals(nativeAddress, metadata.directory.addresses.get(nodeId).nativeAddress);

        DataPlacements dp = metadata.placements;
        assertEquals(1, dp.get(KSM.params.replication).reads.forToken(token).size());
        assertTrue(dp.get(KSM.params.replication).reads.forToken(token).contains(endpoint));
        assertEquals(1, dp.get(KSM.params.replication).writes.forToken(token).size());
        assertTrue(dp.get(KSM.params.replication).writes.forToken(token).contains(endpoint));
    }

    @Test
    public void noRingChanges() throws UnknownHostException
    {
        Keyspaces kss = Keyspaces.NONE.with(KSM);
        DistributedSchema schema = new DistributedSchema(kss);
        Map<InetAddressAndPort, EndpointState> epstates = new HashMap<>();
        for (String state : new String [] {STATUS_BOOTSTRAPPING, STATUS_LEAVING, STATUS_LEAVING, STATUS_BOOTSTRAPPING_REPLACE, REMOVING_TOKEN})
        {
            epstates.put(getByName("127.0.0.2"), withState(state));
            try
            {
                fromEndpointStates(epstates, Murmur3Partitioner.instance, schema);
                fail();
            }
            catch (IllegalStateException e)
            {
                // expected
            }
        }
    }

    @Test
    public void testPlacements() throws UnknownHostException
    {
        int nodes = 10;
        Keyspaces kss = Keyspaces.NONE.with(KSM_NTS);
        DistributedSchema schema = new DistributedSchema(kss);

        Map<Integer, Token> endpoints = new HashMap<>();
        for (int i = 1; i < nodes; i++)
        {
            long t = i * 1000L;
            endpoints.put(i, t(t));
            endpoints.put(++i, t(t + 1));
        }
        Map<InetAddressAndPort, EndpointState> epstates = new HashMap<>();
        for (Map.Entry<Integer, Token> entry : endpoints.entrySet())
        {
            UUID hostId = UUID.randomUUID();
            int num = entry.getKey();
            InetAddressAndPort endpoint = getByName("127.0.0."+num);
            epstates.put(endpoint, epstate(endpoint, endpoint, entry.getValue(), hostId, num % 2 == 1 ? "dc1" : "dc2"));
        }
        ClusterMetadata metadata = fromEndpointStates(epstates, Murmur3Partitioner.instance, schema);
        verifyPlacements(endpoints, metadata);
    }

    private static void verifyPlacements(Map<Integer, Token> endpoints, ClusterMetadata metadata) throws UnknownHostException
    {
        // quick check to make sure cm.placements is populated
        for (Map.Entry<Integer, Token> entry : endpoints.entrySet())
        {
            int num = entry.getKey();
            InetAddressAndPort endpoint = getByName("127.0.0."+num);
            NodeId nodeId = metadata.directory.peerId(endpoint);
            assertEquals(entry.getValue(), metadata.tokenMap.tokens(nodeId).iterator().next());
        }

        PlacementForRange reads = metadata.placements.get(KSM_NTS.params.replication).reads;
        PlacementForRange writes = metadata.placements.get(KSM_NTS.params.replication).writes;
        assertEquals(reads, writes);
        // tokens are
        // dc1: 1: 1000, 3: 3000, 5: 5000, 6: 7000, 7: 9000
        // dc2: 2: 1001, 4: 3001, 6: 5001, 8: 7001, 10:9001

        // token 0 and 10000 should exist on nodes 1, 2, 3, 4, 5, 6
        for (Token t : new Token[] { t(0), t(10000) })
            verify(reads.forToken(t), 1, 2, 3, 4, 5, 6);

        // token 6000 should be on nodes 7, 8, 9, 10, 1, 2;
        verify(reads.forToken(t(6000)), 7, 8, 9, 10, 1, 2);
        // token 7000 should be on nodes 7, 8, 9, 10, 1, 2;
        verify(reads.forToken(t(7000)), 7, 8, 9, 10, 1, 2);
        // token 5001 should be on nodes 6, 7, 8, 9, 10, 1
        verify(reads.forToken(t(5001)), 6, 7, 8, 9, 10, 1);
    }

    private static void verify(EndpointsForToken eps, int ... endpoints) throws UnknownHostException
    {
        assertEquals(endpoints.length, eps.size());
        for (int i : endpoints)
        {
            InetAddressAndPort ep = getByName("127.0.0." + i);
            assertTrue("endpoint "+ep+" should be in " + eps, eps.contains(ep));
        }
    }

    private static EndpointState epstate(InetAddressAndPort internalAddress, InetAddressAndPort nativeAddress, Token token, UUID hostId, String dc)
    {
        Map<ApplicationState, VersionedValue> versionedValues = new EnumMap<>(ApplicationState.class);
        versionedValues.put(STATUS_WITH_PORT, vvf.normal(Collections.singleton(token)));
        versionedValues.put(TOKENS, vvf.tokens(Collections.singleton(token)));
        versionedValues.put(HOST_ID, vvf.hostId(hostId));
        versionedValues.put(DC, vvf.datacenter(dc));
        versionedValues.put(RACK, vvf.datacenter("rack1"));
        versionedValues.put(RELEASE_VERSION, vvf.releaseVersion("3.0.24"));
        versionedValues.put(NATIVE_ADDRESS_AND_PORT, vvf.nativeaddressAndPort(nativeAddress));
        versionedValues.put(INTERNAL_ADDRESS_AND_PORT, vvf.internalAddressAndPort(internalAddress));
        return new EndpointState(new HeartBeatState(1, 1), versionedValues);
    }

    private EndpointState withState(String status) throws UnknownHostException
    {
        EndpointState epstate = epstate(getByName("127.0.0.2"), getByName("127.0.0.2"), t(0), UUID.randomUUID(), "dc1");
        switch (status)
        {
            case STATUS_BOOTSTRAPPING:
                epstate.addApplicationState(STATUS_WITH_PORT, vvf.bootstrapping(Collections.singleton(t(0))));
                break;
            case STATUS_LEAVING:
                epstate.addApplicationState(STATUS_WITH_PORT, vvf.leaving(Collections.singleton(t(0))));
                break;
            case STATUS_MOVING:
                epstate.addApplicationState(STATUS_WITH_PORT, vvf.moving(t(0)));
                break;
            case STATUS_BOOTSTRAPPING_REPLACE:
                epstate.addApplicationState(STATUS_WITH_PORT, vvf.bootReplacing(InetAddress.getByName("127.0.0.3")));
                break;
            case REMOVING_TOKEN:
                epstate.addApplicationState(STATUS_WITH_PORT, vvf.removingNonlocal(UUID.randomUUID()));
                break;
            default:
                throw new IllegalArgumentException("bad status: "+status);
        }
        return epstate;
    }

    private static Token t(long l)
    {
        return new Murmur3Partitioner.LongToken(l);
    }
}
