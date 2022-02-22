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

package org.apache.cassandra.net;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class ConsistencyLevelStartupConnectivityCheckerTest extends AbstractStartupConnectivityCheckerTest
{
    public static final String RACK1 = "rack1";
    public static final String KS1 = "Keyspace1";
    public static final String KS2 = "Keyspace2";
    public static final long TIMEOUT_MILLIS = 100;

    @BeforeClass
    public static void setup() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        Keyspace.setInitialized();
    }

    @Override
    protected void initialize() throws UnknownHostException
    {
        IEndpointSnitch snitch = new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return mapToDatacenter(endpoint);
            }

            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
            {
                return 0;
            }
        };
        DatabaseDescriptor.setEndpointSnitch(snitch);

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        // setup tokenmetadata for all DC
        int dcTokenOffset = 0;
        for (Set<InetAddressAndPort> dcEndpoints : Arrays.asList(dc1Nodes, dc2Nodes, dc3Nodes))
        {
            int token = 1;
            for (InetAddressAndPort ep : dcEndpoints)
            {
                updateMetadata(tk((100L * token++) + dcTokenOffset), ep, metadata);
            }
            dcTokenOffset++;
        }

        // cleanup any leftover user ks
        for (String name : Schema.instance.getUserKeyspaces())
            Schema.instance.transform(ks -> ks.without(name), true, currentTimeMillis());
    }

    private static void updateMetadata(Token token, InetAddressAndPort endpoint, TokenMetadata metadata)
    {
        metadata.updateNormalToken(token, endpoint);
        metadata.updateHostId(UUID.randomUUID(), endpoint);
    }

    // Happy path tests

    @Test
    public void testUnsupportedConsistencyLevel()
    {
        Sink sink = new Sink(peers, peers, peers);
        MessagingService.instance().outboundSink.add(sink);

        List<ConsistencyLevel> unsupportedList = Arrays.asList(ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.TWO,
                                                               ConsistencyLevel.THREE, ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL,
                                                               ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.NODE_LOCAL);

        for (ConsistencyLevel unsupported : unsupportedList)
        {
            assertThatThrownBy(() -> create(unsupported))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("is not supported by ConsistencyLevelStartupConnectivityChecker");
        }

        assertThat(sink.seenConnectionRequests)
        .as("There should be no message sent for unsupported CL types")
        .isEmpty();
    }

    @Test
    public void testWithoutUserKeyspaces()
    {
        for (ConsistencyLevel cl : Arrays.asList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL))
            testHappyPath(Collections.emptyMap(),
                          cl,
                          /* expectedConnectionRequests */ 0,
                          /* description */ cl + ": no peers to contact when there is no user keyspace");
    }

    @Test
    public void happyPath_LOCAL_QUORUM_NetworkTopologyStrategy_RF1()
    {
        // DC1 has 6 hosts, and 5 hosts are primary hosts where the connection is taking place
        testHappyPath(Collections.singletonMap(KS1, "1"),
                      ConsistencyLevel.LOCAL_QUORUM,
                      /* expectedConnectionRequests */ 5,
                      /* description */ "LOCAL_QUORUM 1/1 * 6 should be used for the primary contacts");
    }

    @Test
    public void happyPath_LOCAL_QUORUM_NetworkTopologyStrategy_RF2()
    {
        // DC1 has 6 hosts, and 5 hosts are primary hosts where the connection is taking place
        testHappyPath(Collections.singletonMap(KS1, "2"),
                      ConsistencyLevel.LOCAL_QUORUM,
                      /* expectedConnectionRequests */ 5,
                      /* description */ "LOCAL_QUORUM 2/2 * 6 should be used for the primary contacts");
    }

    @Test
    public void happyPath_LOCAL_QUORUM_NetworkTopologyStrategy_RF3()
    {
        // DC1 has 6 hosts, and 4 hosts are primary hosts where the connection is taking place
        testHappyPath(Collections.singletonMap(KS1, "3"),
                      ConsistencyLevel.LOCAL_QUORUM,
                      /* expectedConnectionRequests */ 4,
                      /* description */ "LOCAL_QUORUM 2/3 * 6 should be used for the primary contacts");
    }

    @Test
    public void happyPath_LOCAL_QUORUM_NetworkTopologyStrategy_RF5()
    {
        // DC1 has 6 hosts, including this host which is bootstrapping, and 3 hosts are primary hosts where the connection is taking place
        testHappyPath(Collections.singletonMap(KS1, "5"),
                      ConsistencyLevel.LOCAL_QUORUM,
                      /* expectedConnectionRequests */ 4,
                      /* description */ "LOCAL_QUORUM 3/5 * 6 should be used for the primary contacts (3 + 1)");
    }

    @Test
    public void happyPath_LOCAL_QUORUM_NetworkTopologyStrategy_MinimumRF()
    {
        // RF of 3 is used and 4 hosts are primary hosts where the connection is taking place
        testHappyPath(ImmutableMap.of(KS1, "3", KS2, "5"),
                      ConsistencyLevel.LOCAL_QUORUM,
                      /* expectedConnectionRequests */ 4,
                      /* description */ "LOCAL_QUORUM 2/3 * 6 should be used for the primary contacts");
    }

    @Test
    public void happyPath_LOCAL_QUORUM_SimpleStrategy_RF3()
    {
        // DC1 has 6 hosts, and 4 hosts are primary hosts where the connection is taking place
        Schema.instance.load(KeyspaceMetadata.create(KS1, KeyspaceParams.simple(3)));

        ConsistencyLevelStartupConnectivityChecker check = create(ConsistencyLevel.LOCAL_QUORUM, false);

        Sink sink = new Sink(dc1NodesMinusLocal, dc1NodesMinusLocal, dc1NodesMinusLocal);
        MessagingService.instance().outboundSink.add(sink);

        assertThat(check.execute(ImmutableSet.copyOf(dc1Nodes), this::mapToDatacenter))
        .as("Checker should pass")
        .isTrue();
        assertThat(sink.seenConnectionRequests)
        .as("SimpleStrategy is unsupported")
        .hasSize(0);
    }

    @Test
    public void happyPath_EACH_QUORUM_NetworkTopologyStrategy_RF3()
    {
        // 4 hosts per DC are primary hosts where the connection is taking place
        testHappyPath(Collections.singletonMap(KS1, "3"),
                      ConsistencyLevel.EACH_QUORUM,
                      /* expectedConnectionRequests */ 12,
                      /* description */ "EACH_QUORUM (2/3 * 6, 2/3 * 6, 2/3 * 6) should be used for the primary contacts");
    }

    @Test
    public void happyPath_EACH_QUORUM_NetworkTopologyStrategy_RF5()
    {
        // 3 hosts in DC1 and 4 hosts in DC 2 and 3 are primary hosts where the connection is taking place
        testHappyPath(Collections.singletonMap(KS1, "5"),
                      ConsistencyLevel.EACH_QUORUM,
                      /* expectedConnectionRequests */ 12,
                      /* description */ "EACH_QUORUM (3/5 * 6, 3/5 * 6, 3/5 * 6) should be used for the primary contacts (3+1, 3+1, 3+1)");
    }

    @Test
    public void happyPath_EACH_QUORUM_NetworkTopologyStrategy_MinimumRF()
    {
        // RF of 3 is used and 12 hosts are primary hosts where the connection is taking place
        testHappyPath(ImmutableMap.of(KS1, "3", KS2, "5"),
                      ConsistencyLevel.EACH_QUORUM,
                      /* expectedConnectionRequests */ 12,
                      /* description */ "EACH_QUORUM (2/3 * 6, 2/3 * 6, 2/3 * 6) should be used for the primary contacts");
    }

    @Test
    public void happyPath_QUORUM_NetworkTopologyStrategy_RF3()
    {
        testHappyPath(Collections.singletonMap(KS1, "3"),
                      ConsistencyLevel.QUORUM,
                      /* expectedConnectionRequests */ 10,
                      /* description */ "QUORUM 5/9 * 18 should be used for the primary contacts");
    }

    @Test
    public void happyPath_QUORUM_NetworkTopologyStrategy_RF5()
    {
        // 3 hosts in DC1 and 4 hosts in DC 2 and 3 are primary hosts where the connection is taking place
        testHappyPath(Collections.singletonMap(KS1, "5"),
                      ConsistencyLevel.QUORUM,
                      /* expectedConnectionRequests */ 11,
                      /* description */ "QUORUM 8/15 * 18 should be used for the primary contacts (8 + 3)");
    }

    @Test
    public void happyPath_QUORUM_NetworkTopologyStrategy_MinimumRF()
    {
        // RF of 3 is used and 16 hosts are primary hosts where the connection is taking place
        testHappyPath(ImmutableMap.of(KS1, "3", KS2, "5"),
                      ConsistencyLevel.QUORUM,
                      /* expectedConnectionRequests */ 10,
                      /* description */ "The mini RF 3-3-3 should be used. QUORUM 5/9 * 18 should be used for the primary contacts (8 + 3)");
    }

    @Test
    public void happyPath_ALL_NetworkTopologyStrategy_RF3()
    {
        // Connects to all nodes, except self
        testHappyPath(Collections.singletonMap(KS1, "3"),
                      ConsistencyLevel.ALL,
                      /* expectedConnectionRequests */ 17,
                      /* description */ "ALL 17 should be used for the primary contacts");
    }

    @Test
    public void happyPath_ALL_NetworkTopologyStrategy_RF5()
    {
        // Connects to all nodes, except self
        testHappyPath(Collections.singletonMap(KS1, "5"),
                      ConsistencyLevel.ALL,
                      /* expectedConnectionRequests */ 17,
                      /* description */ "ALL 17 should be used for the primary contacts");
    }

    @Test
    public void happyPath_ALL_NetworkTopologyStrategy_MinimumRF()
    {
        // Connects to all nodes, except self
        testHappyPath(ImmutableMap.of(KS1, "3", KS2, "5"),
                      ConsistencyLevel.ALL,
                      /* expectedConnectionRequests */ 17,
                      /* description */ "ALL 17 should be used for the primary contacts");
    }

    private void testHappyPath(Map<String, String> keyspaceMap, ConsistencyLevel consistencyLevel, int expectedConnectionRequests, String description)
    {
        // Create keyspaces required for this test
        for (Map.Entry<String, String> entry : keyspaceMap.entrySet())
            createKeyspace(entry.getKey(), entry.getValue());

        ConsistencyLevelStartupConnectivityChecker check = create(consistencyLevel, /* runSecondPhase */ false);
        check.shouldRunSecondPhaseCheck = false;

        Sink sink = new Sink(peers, peers, peers);
        MessagingService.instance().outboundSink.add(sink);

        assertThat(check.execute(peers, this::mapToDatacenter))
        .as("Checker should pass")
        .isTrue();
        assertThat(sink.seenConnectionRequests)
        .as(description)
        .hasSize(expectedConnectionRequests); // excluding itself
    }

    // Some nodes down, but sufficient number of nodes available to fulfill the CL

    @Test
    public void satisfies_LOCAL_QUORUM_NotAlive() throws UnknownHostException
    {
        // Node 1 in DC1 (primary peer) is unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(dc1NodesMinusLocal);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.1"));

        testWithNodeUnavailability(Collections.singletonMap(KS1, "3"),
                                   availablePeers,
                                   peers,
                                   peers,
                                   ConsistencyLevel.LOCAL_QUORUM,
                                   /* expectedConnectionRequests */ 5,
                                   /* isCLSatisfied */ true);
    }

    @Test
    public void satisfies_LOCAL_QUORUM_NoConnectionsAcks() throws UnknownHostException
    {
        // Node 4 in DC1 (primary peer) is unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(dc1NodesMinusLocal);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.4"));

        testWithNodeUnavailability(Collections.singletonMap(KS1, "3"),
                                   peers,
                                   availablePeers,
                                   peers,
                                   ConsistencyLevel.LOCAL_QUORUM,
                                   /* expectedConnectionRequests */ 5,
                                   /* isCLSatisfied */ true);
    }

    @Test
    public void satisfies_EACH_QUORUM_NotAlive() throws UnknownHostException
    {
        // Node 1 in DC1, Node 7 in DC2, and Node 13 in DC3 (primary peers) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.1"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.2.0"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.3.0"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   availablePeers,
                                   peers,
                                   peers,
                                   ConsistencyLevel.EACH_QUORUM,
                                   /* expectedConnectionRequests */ 17,
                                   /* isCLSatisfied */ true);
    }

    @Test
    public void satisfies_EACH_QUORUM_NoConnectionsAcks() throws UnknownHostException
    {
        // Node 1 in DC1, Node 7 in DC2, and Node 13 in DC3 (primary peers) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.1"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.2.0"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.3.0"));

        testWithNodeUnavailability(Collections.singletonMap(KS1, "3"),
                                   peers,
                                   availablePeers,
                                   peers,
                                   ConsistencyLevel.EACH_QUORUM,
                                   /* expectedConnectionRequests */ 17,
                                   /* isCLSatisfied */true);
    }

    @Test
    public void satisfies_QUORUM_NotAlive() throws UnknownHostException
    {
        // Node 1,2,3 in DC1, Node 1 in DC2 (primary peers) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.1"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.2"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.3"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.2.0"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   availablePeers,
                                   peers,
                                   peers,
                                   ConsistencyLevel.QUORUM,
                                   /* expectedConnectionRequests */ 17,
                                   /* isCLSatisfied */ true);
    }

    @Test
    public void satisfies_QUORUM_NoConnectionsAcks() throws UnknownHostException
    {
        // Node 1,2,3 in DC1, Node 1 in DC2 (primary peers) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.1"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.2"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.3"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.2.0"));

        testWithNodeUnavailability(Collections.singletonMap(KS1, "3"),
                                   peers,
                                   availablePeers,
                                   peers,
                                   ConsistencyLevel.QUORUM,
                                   /* exepctedConnnectionRequests */ 17,
                                   /* isCLSatisfied */ true);
    }

    // Some nodes down, and not sufficient number of nodes available to fulfill the CL

    @Test
    public void doesNotsatisfy_LOCAL_QUORUM_NotAlive() throws UnknownHostException
    {
        // Node 3,4 in DC1 (3 is secondary, 4 is primary) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(dc1NodesMinusLocal);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.3"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.4"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   availablePeers,
                                   peers,
                                   peers,
                                   ConsistencyLevel.LOCAL_QUORUM,
                                   /* expectedConnectionRequests */ 5,
                                   /* isCLSatisfied */ false);
    }

    @Test
    public void doesNotsatisfy_LOCAL_QUORUM_NoConnectionsAcks() throws UnknownHostException
    {
        // Node 3,4 in DC1 (3 is secondary, 4 is primary) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(dc1NodesMinusLocal);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.3"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.4"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   peers,
                                   availablePeers,
                                   peers,
                                   ConsistencyLevel.LOCAL_QUORUM,
                                   /* expectedConnectionRequests */ 5,
                                   /* isCLSatisfied */ false);
    }

    @Test
    public void doesNotsatisfy_EACH_QUORUM_NotAlive() throws UnknownHostException
    {
        // Node 0,2 in DC3 (0 is primary, 2 is secondary) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.3.0"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.3.2"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   availablePeers,
                                   peers,
                                   peers,
                                   ConsistencyLevel.EACH_QUORUM,
                                   /* expectedConnectionRequests */ 17,
                                   /* isCLSatisfied */ false);
    }

    @Test
    public void doesNotsatisfy_EACH_QUORUM_NoConnectionsAcks() throws UnknownHostException
    {
        // Node 0,2 in DC3 (0 is primary, 2 is secondary) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.3.0"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.3.2"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   peers,
                                   availablePeers,
                                   peers,
                                   ConsistencyLevel.EACH_QUORUM,
                                   /* expectedConnectionRequests */ 17,
                                   /* isCLSatisfied */ false);
    }

    @Test
    public void doesNotsatisfy_QUORUM_NotAlive() throws UnknownHostException
    {
        // Node 1,2,3 in DC1, Node 0,1 in DC2 (primary peers) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.1"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.2"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.3"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.2.0"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.2.1"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   availablePeers,
                                   peers,
                                   peers,
                                   ConsistencyLevel.QUORUM,
                                   /* expectedConnectionRequests */ 17,
                                   /* isCLSatisfied */ false);
    }

    @Test
    public void doesNotsatisfy_QUORUM_NoConnectionsAcks() throws UnknownHostException
    {
        // Node 1,2,3 in DC1, Node 0,1 in DC2 (primary peers) are unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.1"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.2"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.3"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.2.0"));
        availablePeers.remove(InetAddressAndPort.getByName("127.0.2.1"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   peers,
                                   availablePeers,
                                   peers,
                                   ConsistencyLevel.QUORUM,
                                   /* expectedConnectionRequests */ 17,
                                   /* isCLSatisfied */ false);
    }

    @Test
    public void doesNotsatisfy_ALL_NotAlive() throws UnknownHostException
    {
        // A single node is unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.2.1"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   availablePeers,
                                   peers,
                                   peers,
                                   ConsistencyLevel.ALL,
                                   /* expectedConnectionRequests */ 17,
                                   /* isCLSatisfied */ false);
    }

    @Test
    public void doesNotsatisfy_ALL_NoConnectionsAcks() throws UnknownHostException
    {
        // A single node is unavailable
        Set<InetAddressAndPort> availablePeers = new HashSet<>(peers);
        availablePeers.remove(InetAddressAndPort.getByName("127.0.1.2"));

        Map<String, String> keyspaceMap = Collections.singletonMap(KS1, "3");
        testWithNodeUnavailability(keyspaceMap,
                                   peers,
                                   availablePeers,
                                   peers,
                                   ConsistencyLevel.ALL,
                                   /* expectedConnectionRequests */ 17,
                                   /* isCLSatisfied */ false);
    }

    private void testWithNodeUnavailability(Map<String, String> keyspaceMap,
                                            Set<InetAddressAndPort> hostsThatRespondToGossipRequests,
                                            Set<InetAddressAndPort> hostsThatRespondToAckRequests,
                                            Set<InetAddressAndPort> aliveHosts,
                                            ConsistencyLevel consistencyLevel,
                                            int expectedConnectionRequests,
                                            boolean isCLSatisfied)
    {
        // Create keyspaces required for this test
        for (Map.Entry<String, String> entry : keyspaceMap.entrySet())
            createKeyspace(entry.getKey(), entry.getValue());

        ConsistencyLevelStartupConnectivityChecker check = create(consistencyLevel, /* runSecondPhase */ true);

        Sink sink = new Sink(hostsThatRespondToGossipRequests, hostsThatRespondToAckRequests, aliveHosts);
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertEquals(isCLSatisfied, check.execute(peers, this::mapToDatacenter));
        Assert.assertEquals(expectedConnectionRequests, sink.seenConnectionRequests.size());
    }

    private void createKeyspace(String ksName, String rfInEachDc)
    {
        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put(ReplicationParams.CLASS, NetworkTopologyStrategy.class.getName());

        replicationMap.put("DC1", rfInEachDc);
        replicationMap.put("DC2", rfInEachDc);
        replicationMap.put("DC3", rfInEachDc);

        KeyspaceMetadata meta = KeyspaceMetadata.create(ksName, KeyspaceParams.create(false, replicationMap));
        Schema.instance.load(meta);
    }

    private static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    private static ConsistencyLevelStartupConnectivityChecker create(ConsistencyLevel consistencyLevel)
    {
        return new ConsistencyLevelStartupConnectivityChecker(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS, consistencyLevel);
    }

    private static ConsistencyLevelStartupConnectivityChecker create(ConsistencyLevel consistencyLevel, boolean runSecondPhase)
    {
        ConsistencyLevelStartupConnectivityChecker checker = create(consistencyLevel);
        checker.shouldRunSecondPhaseCheck = runSecondPhase;
        return checker;
    }
}