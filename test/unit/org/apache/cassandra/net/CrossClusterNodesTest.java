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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestAck;
import org.apache.cassandra.gms.GossipDigestAck2;
import org.apache.cassandra.gms.GossipDigestAck2VerbHandler;
import org.apache.cassandra.gms.GossipDigestAckVerbHandler;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.GossipDigestSynVerbHandler;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.EchoVerbHandler;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.gms.ApplicationState.JSON_PAYLOAD;
import static org.apache.cassandra.gms.ApplicationState.JsonPayload.CLUSTER_NAME;
import static org.apache.cassandra.gms.ApplicationState.JsonPayload.PARTITIONER_NAME;
import static org.apache.cassandra.gms.ApplicationState.deserializeJsonPayload;
import static org.apache.cassandra.gms.ApplicationState.serializeJsonPayload;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CrossClusterNodesTest
{
    static final IPartitioner partitioner = new RandomPartitioner();

    private static final Logger testLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static final ListAppender<ILoggingEvent> logListAppender = new ListAppender<>();
    private static VersionedValue ORIG_CLUSTER_NAME;
    private static VersionedValue FOREIGN_CLUSTER_NAME;
    private static final String FOREIGN_PARTITIONER_NAME = "Foreign_Partitioner_Name";

    private final StorageService ss = StorageService.instance;
    private final TokenMetadata tmd = StorageService.instance.getTokenMetadata();
    private final ArrayList<Token> endpointTokens = new ArrayList<>();
    private final ArrayList<Token> keyTokens = new ArrayList<>();
    private final List<InetAddressAndPort> hosts = new ArrayList<>();
    private final List<UUID> hostIds = new ArrayList<>();

    private SeedProvider originalSeedProvider;
    private InetAddressAndPort remoteHostAddress;
    private EndpointState nodeEpState;

    @BeforeClass
    static public void initTest()
    {
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");

        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        logListAppender.start();
        testLogger.addAppender(logListAppender);
        Gossiper.instance.start(0);

        ORIG_CLUSTER_NAME = StorageService.instance.valueFactory.datacenter(StorageService.instance.getClusterName());
        FOREIGN_CLUSTER_NAME = StorageService.instance.valueFactory.datacenter("Foreign_Cluster_Name");
    }

    @Before
    public void setup() throws UnknownHostException
    {
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        tmd.clearUnsafe();

        remoteHostAddress = hosts.get(1);
        nodeEpState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
        logListAppender.start();
    }

    @After
    public void tearDown()
    {
        nodeEpState.addApplicationState(JSON_PAYLOAD, getPayloadWith("", ""));
        DatabaseDescriptor.setSeedProvider(originalSeedProvider);
        logListAppender.list.clear();
    }

    @Test
    public void testSYNHandlerRejectsForeignNode()
    {
        long lastMessageTimestamp = Gossiper.instance.getLastProcessedMessageAt();

        // Bad cluster
        GossipDigestSyn digest = new GossipDigestSyn(FOREIGN_CLUSTER_NAME.value, DatabaseDescriptor.getPartitionerName(), Collections.emptyList());
        Message<GossipDigestSyn> message = Message.synthetic(remoteHostAddress, Verb.SYNC_REQ, digest);
        GossipDigestSynVerbHandler.instance.doVerb(message);
        logListAppender.stop();

        assertTrue(logListAppender.list.stream().anyMatch(l -> l.getFormattedMessage().contains("ClusterName mismatch")));
        assertTrue(logListAppender.list.stream().anyMatch(l -> l.getFormattedMessage().contains(FOREIGN_CLUSTER_NAME.value)));
        assertEquals(lastMessageTimestamp, Gossiper.instance.getLastProcessedMessageAt());

        // Bad partitioner
        logListAppender.start();
        digest = new GossipDigestSyn(DatabaseDescriptor.getClusterName(), FOREIGN_PARTITIONER_NAME, Collections.emptyList());
        message = Message.synthetic(remoteHostAddress, Verb.SYNC_REQ, digest);
        GossipDigestSynVerbHandler.instance.doVerb(message);
        logListAppender.stop();

        assertTrue(logListAppender.list.stream().anyMatch(l -> l.getFormattedMessage().contains("Partitioner mismatch")));
        assertTrue(logListAppender.list.stream().anyMatch(l -> l.getFormattedMessage().contains(FOREIGN_PARTITIONER_NAME)));
        assertEquals(lastMessageTimestamp, Gossiper.instance.getLastProcessedMessageAt());
    }

    @Test
    public void testSYNHandlerPurgesForeignNodes() throws UnknownHostException
    {
        List<GossipDigest> digests = Gossiper.instance.endpointStateMap.keySet().stream().
                                                                                 map(e -> GossipDigest.getUnsafeForTest(e,0,0)).
                                                                                 collect(Collectors.toList());
        digests.add(GossipDigest.getUnsafeForTest(InetAddressAndPort.getByName("127.0.0.3"), 0,0));

        Message<GossipDigestAck> ackReply = GossipDigestSynVerbHandler.createNormalReplyUnsafeForTest(digests);

        assertNull(ackReply.payload.getEndpointStateMapUnsafeForTest().get(InetAddressAndPort.getByName("127.0.0.3")));
    }

    @Test
    public void testSYNHandlerAddsClusterAndPartitioner()
    {
        assertFalse(Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddressAndPort()).
                                      containsApplicationState(JSON_PAYLOAD));

        List<GossipDigest> digests = Gossiper.instance.endpointStateMap.keySet().stream().
                                                                                 map(e -> GossipDigest.getUnsafeForTest(e, 0, 0)).
                                                                                 collect(Collectors.toList());

        Message<GossipDigestAck> ackReply = GossipDigestSynVerbHandler.createNormalReplyUnsafeForTest(digests);

        Map<String, Object> jsonPayload = deserializeJsonPayload(ackReply.
                                                                 payload.
                                                                 getEndpointStateMapUnsafeForTest().
                                                                 get(FBUtilities.getBroadcastAddressAndPort()).
                                                                 getApplicationState(JSON_PAYLOAD));
        assertEquals(jsonPayload.get(CLUSTER_NAME.name()), ORIG_CLUSTER_NAME.value);
        assertEquals(jsonPayload.get(PARTITIONER_NAME.name()), DatabaseDescriptor.getPartitionerName());
    }

    @Test
    public void testACKHandlerRejectsForeignNode()
    {
        GossipDigestAck digest = GossipDigestAck.getInstanceUnsafeForTesting(Collections.emptyList(), ImmutableMap.of(remoteHostAddress, nodeEpState));
        Message<GossipDigestAck> message = Message.synthetic(remoteHostAddress, Verb.GOSSIP_DIGEST_ACK, digest);
        nodeEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithForeignNode());

        long lastMessageTimestamp = Gossiper.instance.getLastProcessedMessageAt();
        GossipDigestAckVerbHandler.instance.doVerb(message);
        logListAppender.stop();

        assertTrue(logListAppender.list.stream().anyMatch(l -> l.getFormattedMessage().contains("Cross cluster node detected")));
        assertEquals(lastMessageTimestamp, Gossiper.instance.getLastProcessedMessageAt());
    }

    @Test
    public void testACKHandlerPurgesForeignNodes() throws UnknownHostException
    {
        EndpointState foreignNodeEpState = fakeState();
        foreignNodeEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithForeignNode());
        Map<InetAddressAndPort, EndpointState> pollutedEpStateMap = new HashMap<>()
        {{
            put(remoteHostAddress, nodeEpState);
            put(InetAddressAndPort.getByName("127.0.0.3"), foreignNodeEpState);
        }};

        GossipDigestAck digest = GossipDigestAck.getInstanceUnsafeForTesting(Collections.emptyList(), pollutedEpStateMap);
        Message<GossipDigestAck> message = Message.synthetic(remoteHostAddress, Verb.GOSSIP_DIGEST_ACK, digest);
        GossipDigestAckVerbHandler.instance.doVerb(message);

        assertNull(pollutedEpStateMap.get(InetAddressAndPort.getByName("127.0.0.3")));
    }

    @Test
    public void testACK2HandlerRejectsForeignNode()
    {
        GossipDigestAck2 digest = GossipDigestAck2.getInstanceUnsafeForTesting(ImmutableMap.of(remoteHostAddress, nodeEpState));
        Message<GossipDigestAck2> message = Message.synthetic(remoteHostAddress, Verb.GOSSIP_DIGEST_ACK2, digest);
        nodeEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithForeignNode());

        long lastMessageTimestamp = Gossiper.instance.getLastProcessedMessageAt();
        GossipDigestAck2VerbHandler.instance.doVerb(message);
        logListAppender.stop();

        assertTrue(logListAppender.list.stream().anyMatch(l -> l.getFormattedMessage().contains("Cross cluster node detected")));
        assertEquals(lastMessageTimestamp, Gossiper.instance.getLastProcessedMessageAt());
    }

    @Test
    public void testACK2HandlerPurgesForeignNodes() throws UnknownHostException
    {
        EndpointState foreignNodeEpState = fakeState();
        foreignNodeEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithForeignNode());
        Map<InetAddressAndPort, EndpointState> pollutedEpStateMap = new HashMap<>()
        {{
            put(remoteHostAddress, nodeEpState);
            put(InetAddressAndPort.getByName("127.0.0.3"), foreignNodeEpState);
        }};

        GossipDigestAck2 digest = GossipDigestAck2.getInstanceUnsafeForTesting(pollutedEpStateMap);
        Message<GossipDigestAck2> message = Message.synthetic(remoteHostAddress, Verb.GOSSIP_DIGEST_ACK2, digest);
        GossipDigestAck2VerbHandler.instance.doVerb(message);

        assertNull(pollutedEpStateMap.get(InetAddressAndPort.getByName("127.0.0.3")));
    }

    @Test
    public void testECHORequest()
    {
        nodeEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithForeignNode());
        Message<NoPayload> message = Message.synthetic(remoteHostAddress, Verb.ECHO_REQ, null);

        EchoVerbHandler.instance.doVerb(message);

        logListAppender.stop();
        assertTrue(logListAppender.list.stream().anyMatch(l -> l.getFormattedMessage().contains("Cross cluster node detected")));
    }

    @Test
    public void testECHOHandler()
    {
        nodeEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithForeignNode());
        Message<NoPayload> message = Message.synthetic(remoteHostAddress, Verb.ECHO_RSP, null);

        ResponseVerbHandler.instance.doVerb(message);

        logListAppender.stop();
        assertTrue(logListAppender.list.stream().anyMatch(l -> l.getFormattedMessage().contains("Cross cluster node detected")));
    }

    @Test
    public void testApplyStateLocally()
    {
        nodeEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithForeignNode());
        Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, nodeEpState));
        logListAppender.stop();
        assertTrue(logListAppender.list.stream().anyMatch(l -> l.getFormattedMessage().contains("Cross cluster node detected")));
    }

    @Test
    public void testMaybeBelongsInCluster()
    {
        EndpointState testEpState = fakeState();

        // Works with no data
        assertTrue(Gossiper.maybeBelongsInCluster(remoteHostAddress, testEpState));

        // Works with data
        testEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithNonForeignNode());
        assertTrue(Gossiper.maybeBelongsInCluster(remoteHostAddress, testEpState));

        // Rejects bad cluster
        testEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithForeignNode());
        assertFalse(Gossiper.maybeBelongsInCluster(remoteHostAddress, testEpState));

        // Rejects bad partitioner
        testEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithBadPartitioner());
        assertFalse(Gossiper.maybeBelongsInCluster(remoteHostAddress, testEpState));
    }

    @Test
    public void testRemoveCrossClusterNodes() throws UnknownHostException
    {
        EndpointState foreignNodeEpState = fakeState();
        foreignNodeEpState.addApplicationState(JSON_PAYLOAD, getPayloadWithForeignNode());
        Map<InetAddressAndPort, EndpointState> pollutedEpStateMap = new HashMap<>()
        {{
            put(InetAddressAndPort.getByName("127.0.0.3"), foreignNodeEpState);
        }};
        pollutedEpStateMap.putAll(Gossiper.instance.endpointStateMap);

        Map<InetAddressAndPort, EndpointState> sanitizedEpStateMap = Gossiper.removeForeignClusterNodes(pollutedEpStateMap);

        assertEquals(sanitizedEpStateMap.size(), Gossiper.instance.endpointStateMap.size());
        assertNull(pollutedEpStateMap.get(InetAddressAndPort.getByName("127.0.0.3")));
    }

    private static VersionedValue getPayloadWithForeignNode()
    {
        return getPayloadWith(CLUSTER_NAME.name(), FOREIGN_CLUSTER_NAME.value);
    }

    private static VersionedValue getPayloadWithNonForeignNode()
    {
        return getPayloadWith(CLUSTER_NAME.name(), ORIG_CLUSTER_NAME.value);
    }

    private static VersionedValue getPayloadWithBadPartitioner()
    {
        return getPayloadWith(PARTITIONER_NAME.name(), FOREIGN_PARTITIONER_NAME);
    }

    private static VersionedValue getPayloadWith(String key, String value)
    {
        Map<String, Object> payload = new HashMap<>()
        {{
            put(key, value);
        }};

        return serializeJsonPayload(payload);
    }

    private static EndpointState fakeState()
    {
        return new EndpointState(new HeartBeatState((int) System.currentTimeMillis()/1000, 1234));
    }
}
