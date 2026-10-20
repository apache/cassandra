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

package org.apache.cassandra.service.reads.tracked;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ReadReconciliationsTest
{
    private static final TableId TABLE_ID = TableId.generate();
    private static final CoordinatorLogId LOG_1 = CoordinatorLogId.fromLong(CoordinatorLogId.asLong(1, 1));
    private static final CoordinatorLogId LOG_2 = CoordinatorLogId.fromLong(CoordinatorLogId.asLong(2, 1));
    private static int LOCAL_NODE_ID;
    private static String LOCAL_DC;
    private static Range<Token> FULL_RANGE;
    private static final String REMOTE_DC = "remote_dc";
    private static InetAddressAndPort NODE_2;
    private static InetAddressAndPort NODE_3;
    private static InetAddressAndPort NODE_4;
    private static InetAddressAndPort REMOTE_DC_NODE;

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        LOCAL_NODE_ID = ClusterMetadata.current().myNodeId().id();
        LOCAL_DC = ClusterMetadata.current().locator.local().datacenter;
        NODE_2 = InetAddressAndPort.getByNameUnchecked("127.0.0.2");
        NODE_3 = InetAddressAndPort.getByNameUnchecked("127.0.0.3");
        NODE_4 = InetAddressAndPort.getByNameUnchecked("127.0.0.4");
        REMOTE_DC_NODE = InetAddressAndPort.getByNameUnchecked("127.0.0.5");
        Token minToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        FULL_RANGE = new Range<>(minToken, minToken);
        ClusterMetadataTestHelper.register(NODE_2, LOCAL_DC, "rack0");
        ClusterMetadataTestHelper.register(NODE_3, LOCAL_DC, "rack0");
        ClusterMetadataTestHelper.register(NODE_4, LOCAL_DC, "rack0");
        ClusterMetadataTestHelper.register(REMOTE_DC_NODE, REMOTE_DC, "rack0");
    }

    private static MutationSummary summary(CoordinatorLogId logId, int[] reconciled, int[] unreconciled)
    {
        MutationSummary.Builder builder = new MutationSummary.Builder(TABLE_ID);
        MutationSummary.CoordinatorSummary.Builder logBuilder = builder.builderForLog(logId);
        for (int i = 0; i < reconciled.length; i += 2)
            logBuilder.reconciled.add(reconciled[i], reconciled[i + 1]);
        for (int i = 0; i < unreconciled.length; i += 2)
            logBuilder.unreconciled.add(unreconciled[i], unreconciled[i + 1]);
        return builder.build();
    }

    @Test
    public void testCoordinatorTracksPerPeerPendingMutations()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 1001L);
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2, 3 };

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        MutationSummary summaryPeer2 = summary(LOG_2, new int[]{}, new int[]{ 10, 10 });
        MutationSummary summaryPeer3 = summary(LOG_2, new int[]{}, new int[]{ 10, 10, 20, 20 });

        coordinator.acceptRemoteSummary(summaryPeer2, 2, true);
        coordinator.acceptRemoteSummary(summaryPeer3, 3, true);

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        ReadReconciliations.Coordinator.PeerState peer3 = coordinator.getPeerState(3);

        assertNotNull(peer2);
        assertNotNull(peer3);

        assertTrue(peer2.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertEquals(1, peer2.pendingLocalMutations.idCount());

        assertTrue(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertTrue(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 20)));
        assertEquals(2, peer3.pendingLocalMutations.idCount());

        coordinator.acceptMutation(new ShortMutationId(LOG_2, 10));

        assertFalse(peer2.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertTrue(peer2.pendingLocalMutations.isEmpty());

        assertFalse(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertTrue(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 20)));
        assertEquals(1, peer3.pendingLocalMutations.idCount());

        coordinator.acceptMutation(new ShortMutationId(LOG_2, 20));
        assertTrue(peer3.pendingLocalMutations.isEmpty());
    }

    @Test
    public void testMutationReceivedBeforeSecondPeerSummaryArrives()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 1002L);
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2, 3 };

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        MutationSummary summaryPeer2 = summary(LOG_2, new int[]{}, new int[]{ 10, 10 });
        MutationSummary summaryPeer3 = summary(LOG_2, new int[]{}, new int[]{ 10, 10 });

        coordinator.acceptRemoteSummary(summaryPeer2, 2, true);

        // Deliver mutation 10 before peer 3's summary arrives
        coordinator.acceptMutation(new ShortMutationId(LOG_2, 10));

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        assertTrue(peer2.pendingLocalMutations.isEmpty());

        // Now peer 3's summary arrives with mutation 10
        coordinator.acceptRemoteSummary(summaryPeer3, 3, true);

        ReadReconciliations.Coordinator.PeerState peer3 = coordinator.getPeerState(3);
        assertNotNull(peer3);

        // Peer 3 should NOT have mutation 10 pending because it was already completed during this read
        assertFalse(peer3.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 10)));
        assertTrue(peer3.pendingLocalMutations.isEmpty());
    }

    @Test
    public void testMutationArrivesLocallyDuringCallbackRegistration()
    {
        // Tests the race window where a mutation was missing when acceptRemoteSummary was called,
        // but was witnessed locally before/during callback registration.
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 1003L);
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2 };

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        MutationSummary summaryPeer2 = summary(LOG_2, new int[]{}, new int[]{ 50, 50 });
        coordinator.acceptRemoteSummary(summaryPeer2, 2, true);

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        assertNotNull(peer2);
        assertTrue(peer2.pendingLocalMutations.contains(new ShortMutationId(LOG_2, 50)));

        // Once the mutation arrives, pendingLocalMutations is immediately updated
        coordinator.acceptMutation(new ShortMutationId(LOG_2, 50));
        assertTrue(peer2.pendingLocalMutations.isEmpty());
    }

    @Test
    public void testSummaryNodeSkipsAckWhenAlreadyInSync()
    {
        // On a summary node (LOCAL_NODE_ID != dataNode)
        int dataNode = 2;
        int[] summaryNodes = new int[]{ LOCAL_NODE_ID, 3 };
        TrackedRead.Id readId = new TrackedRead.Id(dataNode, 2001L);

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        // Local summary has offsets 1..10
        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 10 }, new int[]{});
        // Data node summary has offsets 1..5
        MutationSummary dataSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});

        coordinator.acceptRemoteSummary(dataSummary, dataNode, true);
        coordinator.maybeSendPerPeerAcksOnSummaryNode();

        ReadReconciliations.Coordinator.PeerState dataPeer = coordinator.getPeerState(dataNode);
        assertNotNull(dataPeer);
        // Set local summary directly to avoid network send in acceptLocalSummary
        coordinator.setLocalSummaryForTesting(localSummary);
        coordinator.maybeSendPerPeerAcksOnSummaryNode();

        // Because localSummary already has all unreconciled offsets from dataSummary (0 missing),
        // ackSent should be marked true without sending any message
        assertTrue(dataPeer.ackSent);
    }

    @Test
    public void testSummaryNodeSendsPerPeerAckWhenMissingPullsComplete()
    {
        int dataNode = 2;
        int[] summaryNodes = new int[]{ LOCAL_NODE_ID, 3 };
        TrackedRead.Id readId = new TrackedRead.Id(dataNode, 2002L);

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        // Local summary has offset 1..5
        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});
        // Data node summary has unreconciled offset 10..10 (missing locally)
        MutationSummary dataSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{ 10, 10 });

        coordinator.setLocalSummaryForTesting(localSummary);
        coordinator.acceptRemoteSummary(dataSummary, dataNode, true);

        ReadReconciliations.Coordinator.PeerState dataPeer = coordinator.getPeerState(dataNode);
        assertNotNull(dataPeer);
        // Mutation 10 is missing, so ackSent should be false until mutation arrives
        assertFalse(dataPeer.ackSent);
        assertTrue(dataPeer.pendingLocalMutations.contains(new ShortMutationId(LOG_1, 10)));

        // Once mutation 10 arrives, the ack is sent
        coordinator.acceptMutation(new ShortMutationId(LOG_1, 10));
        assertTrue(dataPeer.ackSent);
        assertTrue(dataPeer.pendingLocalMutations.isEmpty());
    }

    @Test
    public void testDataNodeEagerCompletionSkipsAckForUpToDateSummary()
    {
        // On dataNode, requiredSummaries = 1 (e.g. CL.QUORUM with 2 contacted summary nodes)
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2, 3 };
        TrackedRead.Id readId = new TrackedRead.Id(dataNode, 3001L);

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes, 1);

        // Local summary has offsets 1..5
        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});
        // Peer 2 summary already has offsets 1..5 (mutually in sync)
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});

        coordinator.setLocalSummaryForTesting(localSummary);
        assertFalse(coordinator.isReadCompleted());

        // Peer 2's summary arrives -> quorum is satisfied immediately without waiting for an ack from peer 2
        // and without waiting for peer 3's summary
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);
        assertTrue(coordinator.isReadCompleted());
    }

    @Test
    public void testDataNodePivotsFromSlowNodeToReadyNode()
    {
        // On dataNode, requiredSummaries = 1, contacted nodes = 2 and 3
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2, 3 };
        TrackedRead.Id readId = new TrackedRead.Id(dataNode, 3002L);

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes, 1);

        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});
        // Peer 2 has an unreconciled mutation 10 that dataNode is missing
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{ 10, 10 });
        // Peer 3 is in sync with dataNode
        MutationSummary peer3Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});

        coordinator.setLocalSummaryForTesting(localSummary);

        // Peer 2's summary arrives first
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);
        assertFalse(coordinator.isReadCompleted());

        coordinator.acceptRemoteSummary(peer3Summary, 3, true);
        assertTrue(coordinator.isReadCompleted());

        assertFalse(coordinator.isCoordinatorCompleted());

        coordinator.acceptMutation(new ShortMutationId(LOG_1, 10));
        assertTrue(coordinator.isCoordinatorCompleted());
    }

    @Test
    public void testDataNodeAcceptsLegacyAckFromUnupgradedSummaryNode()
    {
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2 };
        TrackedRead.Id readId = new TrackedRead.Id(dataNode, 3003L);

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes, 1);

        // Local summary has unreconciled 10, which peer 2 is missing
        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{ 10, 10 });
        // Peer 2 only has 1..5, missing 10
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});

        coordinator.setLocalSummaryForTesting(localSummary);
        // Peer 2 is un-upgraded (supportsPerPeerAck = false)
        coordinator.acceptRemoteSummary(peer2Summary, 2, false);

        assertFalse(coordinator.isReadCompleted());

        // Peer 2 sends legacy ack (peerNode = null)
        coordinator.acceptSyncAck(2, null);

        // Now read is completed
        assertTrue(coordinator.isReadCompleted());
    }

    @Test
    public void testCoordinatorRejectsAckFromUnknownPeer()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 3004L);
        int dataNode = LOCAL_NODE_ID;
        int[] summaryNodes = new int[]{ 2 };

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes, 1);

        try
        {
            // Node 99 is not in summaryNodes
            coordinator.acceptSyncAck(99, null);
            Assert.fail("Expected IllegalStateException for ack from unknown peer");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("Received reconcile ack from unknown peer 99"));
        }
    }

    @Test
    public void testRemoteSummaryArrivingBeforeLocalSummaryUpdatesQuorumChecker()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 3005L);
        int[] summaryNodes = new int[]{ 2, 3 };

        MutationSummary localSummary = new MutationSummary.Builder(TABLE_ID).build();
        MutationSummary peer2Summary = new MutationSummary.Builder(TABLE_ID).build();
        TrackedSummaryResponse response = new TrackedSummaryResponse(readId, peer2Summary, LOCAL_NODE_ID, summaryNodes);

        // Remote summary arrives before local summary completes
        ReadReconciliations.instance.acceptRemoteSummary(InetAddressAndPort.getByNameUnchecked("127.0.0.2"), response, true);

        ReadReconciliations.Coordinator coordinator = ReadReconciliations.instance.getCoordinator(readId);
        assertNotNull(coordinator);
        assertFalse(coordinator.isReadCompleted());

        // Local summary completes and supplies quorum checker requiring 2 endpoints (local node + 1 peer)
        ReadReconciliations.instance.acceptLocalSummary(readId, localSummary, summaryNodes, ReadReconciliations.QuorumChecker.simple(2));

        // Eager quorum completion should succeed immediately with local node + peer 2
        assertTrue(coordinator.isReadCompleted());
    }

    @Test
    public void testUnresponsiveNonQuorumPeerAllowsEagerReadCompletion()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 4001L);
        int[] summaryNodes = new int[]{ 2, 3 };

        MutationSummary localSummary = new MutationSummary.Builder(TABLE_ID).build();
        MutationSummary peer2Summary = new MutationSummary.Builder(TABLE_ID).build();
        TrackedSummaryResponse response = new TrackedSummaryResponse(readId, peer2Summary, LOCAL_NODE_ID, summaryNodes);

        ReadReconciliations.instance.acceptRemoteSummary(InetAddressAndPort.getByNameUnchecked("127.0.0.2"), response, true);

        ReadReconciliations.Coordinator coordinator = ReadReconciliations.instance.getCoordinator(readId);
        assertNotNull(coordinator);

        // Local summary completes with quorum requirement of 2 (local node + peer 2).
        // Peer 3 is unresponsive, but does not block eager read completion.
        ReadReconciliations.instance.acceptLocalSummary(readId, localSummary, summaryNodes, ReadReconciliations.QuorumChecker.simple(2));

        assertTrue(coordinator.isReadCompleted());
        // Coordinator cleanup waits for all contacted summaries, so unresponsive peer 3 leaves it until purger runs
        assertFalse(coordinator.isCoordinatorCompleted());
        assertNotNull(ReadReconciliations.instance.getCoordinator(readId));

        // Purger removes the coordinator after expiration
        long futureNanos = Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        int purged = ReadReconciliations.instance.expire(futureNanos);
        assertTrue(purged >= 1);
        assertNull(ReadReconciliations.instance.getCoordinator(readId));
    }

    @Test
    public void testDataNodeAloneSatisfiesQuorumWithoutWaitingForSummaryNodes()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 4010L);
        int[] summaryNodes = { 2 };

        MutationSummary localSummary = new MutationSummary.Builder(TABLE_ID).build();
        ReadReconciliations.instance.acceptLocalSummary(readId, localSummary, summaryNodes, ReadReconciliations.QuorumChecker.simple(1));

        ReadReconciliations.Coordinator coordinator = ReadReconciliations.instance.getCoordinator(readId);
        assertNotNull(coordinator);
        assertTrue(coordinator.isReadCompleted());
        assertFalse(coordinator.isCoordinatorCompleted());

        ReadReconciliations.instance.expire(Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(60));
        assertNull(ReadReconciliations.instance.getCoordinator(readId));
    }

    @Test
    public void testTrailingSummaryAfterEagerReadCompletionDoesNotResurrectZombieCoordinator()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 4004L);
        int[] summaryNodes = new int[]{ 2, 3 };

        MutationSummary localSummary = new MutationSummary.Builder(TABLE_ID).build();
        MutationSummary peer2Summary = new MutationSummary.Builder(TABLE_ID).build();
        TrackedSummaryResponse response2 = new TrackedSummaryResponse(readId, peer2Summary, LOCAL_NODE_ID, summaryNodes);

        ReadReconciliations.instance.acceptRemoteSummary(InetAddressAndPort.getByNameUnchecked("127.0.0.2"), response2, true);

        ReadReconciliations.Coordinator coordinator = ReadReconciliations.instance.getCoordinator(readId);
        assertNotNull(coordinator);

        // Eager quorum satisfied with local node + peer 2
        ReadReconciliations.instance.acceptLocalSummary(readId, localSummary, summaryNodes, ReadReconciliations.QuorumChecker.simple(2));

        assertTrue(coordinator.isReadCompleted());
        assertFalse(coordinator.isCoordinatorCompleted());
        assertSame(coordinator, ReadReconciliations.instance.getCoordinator(readId));

        // Slower peer 3 summary delivers later
        MutationSummary peer3Summary = new MutationSummary.Builder(TABLE_ID).build();
        TrackedSummaryResponse response3 = new TrackedSummaryResponse(readId, peer3Summary, LOCAL_NODE_ID, summaryNodes);
        ReadReconciliations.instance.acceptRemoteSummary(InetAddressAndPort.getByNameUnchecked("127.0.0.3"), response3, true);

        // Existing coordinator completes and is removed, with no zombie coordinator left behind
        assertTrue(coordinator.isCoordinatorCompleted());
        assertNull(ReadReconciliations.instance.getCoordinator(readId));
    }

    @Test
    public void testDataNodeWaitsForPendingPullsBeforeCleanup()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 4002L);
        int[] summaryNodes = new int[]{ 2 };

        // Local summary is missing mutation 10 from peer 2
        MutationSummary localSummary = summary(LOG_1, new int[]{}, new int[]{});
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{ 10, 10 });
        TrackedSummaryResponse response2 = new TrackedSummaryResponse(readId, peer2Summary, LOCAL_NODE_ID, summaryNodes);

        ReadReconciliations.instance.acceptRemoteSummary(InetAddressAndPort.getByNameUnchecked("127.0.0.2"), response2, true);

        ReadReconciliations.Coordinator coordinator = ReadReconciliations.instance.getCoordinator(readId);
        assertNotNull(coordinator);

        ReadReconciliations.instance.acceptLocalSummary(readId, localSummary, summaryNodes, ReadReconciliations.QuorumChecker.simple(2));

        // Quorum of 2 requires peer 2, but local pull for offset 10 is still pending,
        // so read has not yet completed and coordinator is not yet removed
        assertFalse(coordinator.isReadCompleted());
        assertFalse(coordinator.isCoordinatorCompleted());
        assertNotNull(ReadReconciliations.instance.getCoordinator(readId));

        // When mutation 10 arrives, pending pulls become empty, quorum is satisfied,
        // read completes, and coordinator completes
        ReadReconciliations.instance.acceptMutation(readId, new ShortMutationId(LOG_1, 10));
        assertTrue(coordinator.isReadCompleted());
        assertTrue(coordinator.isCoordinatorCompleted());
        assertNull(ReadReconciliations.instance.getCoordinator(readId));
    }

    @Test
    public void testExpiredCoordinatorPurgedByPurger()
    {
        int dataNode = 2;
        TrackedRead.Id readId = new TrackedRead.Id(dataNode, 4003L);
        int[] summaryNodes = new int[]{ LOCAL_NODE_ID, 3 };

        MutationSummary dataSummary = new MutationSummary.Builder(TABLE_ID).build();
        TrackedSummaryResponse response = new TrackedSummaryResponse(readId, dataSummary, dataNode, summaryNodes);

        ReadReconciliations.instance.acceptRemoteSummary(InetAddressAndPort.getByNameUnchecked("127.0.0.2"), response, true);

        ReadReconciliations.Coordinator coordinator = ReadReconciliations.instance.getCoordinator(readId);
        assertNotNull(coordinator);
        assertFalse(coordinator.isCoordinatorCompleted());

        // Advance nanoTime past expiration and verify purger removes the coordinator
        long futureNanos = Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        int purged = ReadReconciliations.instance.expire(futureNanos);
        assertTrue(purged >= 1);
        assertNull(ReadReconciliations.instance.getCoordinator(readId));
    }

    @Test
    public void testPerPeerAckEnablesQuorumWhenSummaryHasMissingOffsets()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 4004L);
        int[] summaryNodes = new int[]{ 2 };

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, LOCAL_NODE_ID, summaryNodes, 1);

        // Local summary has unreconciled 10, which peer 2 is missing
        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{ 10, 10 });
        // Peer 2 only has 1..5, missing 10
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});

        coordinator.setLocalSummaryForTesting(localSummary);
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);

        // Read not completed yet because peer 2 summary lacks offset 10 and has not sent ack for LOCAL_NODE_ID
        assertFalse(coordinator.isReadCompleted());

        // Peer 2 sends per-peer ack covering local node
        coordinator.acceptSyncAck(2, LOCAL_NODE_ID);

        // Read completes because ack marks peer 2 synchronized with data node
        assertTrue(coordinator.isReadCompleted());
    }

    @Test
    public void testSummaryNodeFallbackToLegacySingleAckMode()
    {
        // On summary node (LOCAL_NODE_ID), dataNode is 2, contacted summary nodes are LOCAL_NODE_ID and 3
        int dataNode = 2;
        int[] summaryNodes = new int[]{ LOCAL_NODE_ID, 3 };
        TrackedRead.Id readId = new TrackedRead.Id(dataNode, 5001L);

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        // Local summary has offsets 1..5
        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});
        // Legacy data node has unreconciled offset 10 (missing locally)
        MutationSummary dataSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{ 10, 10 });
        // Peer 3 summary is in sync with 1..5
        MutationSummary peer3Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});

        AtomicReference<Message<?>> sentAck =
            new AtomicReference<>();
        MessagingService.instance().outboundSink.add((msg, to) -> {
            if (msg.verb() == Verb.MT_READ_RECONCILE_ACK)
                sentAck.set(msg);
            return false;
        });

        try
        {
            coordinator.setLocalSummaryForTesting(localSummary);

            // Data node summary arrives with supportsPerPeerAck = false (legacy mode)
            coordinator.acceptRemoteSummary(dataSummary, dataNode, false);

            ReadReconciliations.Coordinator.PeerState dataPeer = coordinator.getPeerState(dataNode);
            assertNotNull(dataPeer);
            assertFalse(dataPeer.supportsPerPeerAck);
            assertFalse(dataPeer.ackSent);
            assertTrue(dataPeer.pendingLocalMutations.contains(new ShortMutationId(LOG_1, 10)));
            assertFalse(coordinator.isCoordinatorCompleted());
            assertNull(sentAck.get());

            // Peer 3 summary arrives
            coordinator.acceptRemoteSummary(peer3Summary, 3, false);

            // Still not completed because mutation 10 is missing
            assertFalse(coordinator.isCoordinatorCompleted());
            assertNull(sentAck.get());

            // Missing mutation 10 arrives from pull
            coordinator.acceptMutation(new ShortMutationId(LOG_1, 10));

            // Per-peer ack should NOT have been sent
            assertFalse(dataPeer.ackSent);

            // Summary node must have completed and sent the single legacy ack
            assertTrue(coordinator.isCoordinatorCompleted());
            assertNotNull(sentAck.get());
            assertEquals(Verb.MT_READ_RECONCILE_ACK, sentAck.get().verb());
            assertNull(sentAck.get().header.params().get(ParamType.MT_RECONCILE_PEER));
            assertEquals(readId, ((ReadReconcileAck) sentAck.get().payload).readId);
        }
        finally
        {
            MessagingService.instance().outboundSink.clear();
        }
    }

    @Test
    public void testSummaryResponseAndPerPeerAckCarryTracingParams()
    {
        int dataNode = 2;
        int[] summaryNodes = { LOCAL_NODE_ID, 3 };
        TrackedRead.Id readId = new TrackedRead.Id(dataNode, 5002L);

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, dataNode, summaryNodes);

        MutationSummary localSummary = new MutationSummary.Builder(TABLE_ID).build();
        MutationSummary dataSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{ 10, 10 });

        AtomicReference<Message<?>> sentSummary = new AtomicReference<>();
        AtomicReference<Message<?>> sentAck = new AtomicReference<>();
        MessagingService.instance().outboundSink.add((msg, to) -> {
            if (msg.verb() == Verb.MT_SUMMARY_RSP)
                sentSummary.set(msg);
            else if (msg.verb() == Verb.MT_READ_RECONCILE_ACK)
                sentAck.set(msg);
            return false;
        });

        Tracing.instance.newSession(Tracing.TraceType.QUERY);
        try
        {
            coordinator.acceptLocalSummary(localSummary, null);
            coordinator.acceptRemoteSummary(dataSummary, dataNode, true);
            coordinator.acceptMutation(new ShortMutationId(LOG_1, 10));

            assertNotNull(sentSummary.get());
            assertEquals(Tracing.instance.getSessionId(), sentSummary.get().header.params().get(ParamType.TRACE_SESSION));
            assertNotNull(sentAck.get());
            assertEquals(dataNode, sentAck.get().header.params().get(ParamType.MT_RECONCILE_PEER));
            assertEquals(Tracing.instance.getSessionId(), sentAck.get().header.params().get(ParamType.TRACE_SESSION));
        }
        finally
        {
            Tracing.instance.set(null);
            MessagingService.instance().outboundSink.clear();
        }
    }

    @Test
    public void testMutualQuorumCandidatePruning()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 6001L);
        int[] summaryNodes = new int[]{ 2, 3, 4 };

        // Require dataNode + 2 summary nodes
        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, LOCAL_NODE_ID, summaryNodes, 2);

        // Data node has offsets 1..5 and unreconciled offset 10
        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{ 10, 10 });
        // Peer 2 has only 1..5, missing offset 10 (not synced with dataNode)
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});
        // Peer 3 has 1..5 and 10 (reconciled), plus unreconciled offset 20 (synced with dataNode once pull of 20 completes)
        MutationSummary peer3Summary = summary(LOG_1, new int[]{ 1, 5, 10, 10 }, new int[]{ 20, 20 });
        // Peer 4 has 1..5 and 10 (reconciled), unreconciled empty (synced with dataNode, but missing Peer 3's offset 20)
        MutationSummary peer4Summary = summary(LOG_1, new int[]{ 1, 5, 10, 10 }, new int[]{});

        coordinator.setLocalSummaryForTesting(localSummary);
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);
        coordinator.acceptRemoteSummary(peer3Summary, 3, true);
        coordinator.acceptRemoteSummary(peer4Summary, 4, true);

        // Data node pull of mutation 20 completes
        coordinator.acceptMutation(new ShortMutationId(LOG_1, 20));

        // Read cannot complete:
        // - Peer 2 is pruned because it is not synchronized with dataNode.
        // - Peer 3 and Peer 4 are both synchronized with dataNode, but not mutually synchronized (Peer 4 lacks offset 20).
        assertFalse(coordinator.isReadCompleted());

        // Peer 4 syncs with Peer 3
        coordinator.acceptSyncAck(4, 3);

        // Read now completes with quorum {dataNode, 3, 4}
        assertTrue(coordinator.isReadCompleted());
    }

    @Test
    public void testPairwiseSyncPrecalculatedWhenRemoteSummaryArrivesFirst()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 7001L);
        int[] summaryNodes = new int[]{ 2 };

        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, LOCAL_NODE_ID, summaryNodes, 1);

        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{});

        // Remote summary arrives before local summary
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);
        assertFalse(coordinator.isReadCompleted());

        // Local summary arrives: pairwise sync is precalculated and read completes
        coordinator.setLocalSummaryForTesting(localSummary);
        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        assertNotNull(peer2);
        assertTrue(peer2.syncedPeers.contains(LOCAL_NODE_ID));
    }

    @Test
    public void testPairwiseMutualSyncSummaryDrivenQuorumSizeGreaterThanOne()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 7002L);
        int[] summaryNodes = new int[]{ 2, 3, 4 };

        // Require dataNode plus 2 summary nodes
        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, LOCAL_NODE_ID, summaryNodes, 2);

        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 10 }, new int[]{});
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 6 }, new int[]{ 7, 10 });
        MutationSummary peer3Summary = summary(LOG_1, new int[]{ 1, 8 }, new int[]{ 9, 10 });
        MutationSummary peer4Summary = summary(LOG_1, new int[]{ 1, 9 }, new int[]{ 10, 10 });

        coordinator.setLocalSummaryForTesting(localSummary);
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);
        coordinator.acceptRemoteSummary(peer3Summary, 3, true);
        coordinator.acceptRemoteSummary(peer4Summary, 4, true);

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        ReadReconciliations.Coordinator.PeerState peer3 = coordinator.getPeerState(3);
        ReadReconciliations.Coordinator.PeerState peer4 = coordinator.getPeerState(4);

        assertNotNull(peer2);
        assertNotNull(peer3);
        assertNotNull(peer4);

        // Verify mutual sync between all pairs is established directly from summaries without explicit sync acks
        assertTrue(peer2.syncedPeers.contains(LOCAL_NODE_ID));
        assertTrue(peer2.syncedPeers.contains(3));
        assertTrue(peer2.syncedPeers.contains(4));

        assertTrue(peer3.syncedPeers.contains(LOCAL_NODE_ID));
        assertTrue(peer3.syncedPeers.contains(2));
        assertTrue(peer3.syncedPeers.contains(4));

        assertTrue(peer4.syncedPeers.contains(LOCAL_NODE_ID));
        assertTrue(peer4.syncedPeers.contains(2));
        assertTrue(peer4.syncedPeers.contains(3));

        // Read cannot complete yet while local pulls for offsets 7..10 are pending
        assertFalse(coordinator.isReadCompleted());

        // Complete local pulls for 7..10
        for (int offset = 7; offset <= 10; offset++)
            coordinator.acceptMutation(new ShortMutationId(LOG_1, offset));

        // Read completes now that candidate quorum of size 2 is ready and mutually synchronized
        assertTrue(coordinator.isReadCompleted());
    }

    @Test
    public void testPairwiseMutualSyncPartialCliqueSelection()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 7003L);
        int[] summaryNodes = new int[]{ 2, 3, 4 };

        // Require dataNode plus 2 summary nodes
        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, LOCAL_NODE_ID, summaryNodes, 2);

        // Data node covers all reconciled mutations
        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 30 }, new int[]{});

        // Peer 2 and 3 are mutually synchronized:
        // Peer 2 unreconciled [11, 15] is in Peer 3 unreconciled.
        // Peer 3 unreconciled [21, 25] is in Peer 2 reconciled.
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 10, 21, 25 }, new int[]{ 11, 15 });
        MutationSummary peer3Summary = summary(LOG_1, new int[]{ 1, 10 }, new int[]{ 11, 15, 21, 25 });

        // Peer 4 has unreconciled [26, 30], which neither Peer 2 nor Peer 3 covers
        MutationSummary peer4Summary = summary(LOG_1, new int[]{ 1, 10 }, new int[]{ 21, 25, 26, 30 });

        coordinator.setLocalSummaryForTesting(localSummary);
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);
        coordinator.acceptRemoteSummary(peer4Summary, 4, true);

        // Accept local pulls for peer 2 and peer 4
        for (int offset = 11; offset <= 15; offset++)
            coordinator.acceptMutation(new ShortMutationId(LOG_1, offset));
        for (int offset = 21; offset <= 30; offset++)
            coordinator.acceptMutation(new ShortMutationId(LOG_1, offset));

        // Only peers 2 and 4 are present; they are not mutually synchronized
        assertFalse(coordinator.isReadCompleted());

        // Peer 3 arrives; candidate quorum {2, 3} satisfies mutual synchronization
        coordinator.acceptRemoteSummary(peer3Summary, 3, true);

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        ReadReconciliations.Coordinator.PeerState peer3 = coordinator.getPeerState(3);
        ReadReconciliations.Coordinator.PeerState peer4 = coordinator.getPeerState(4);

        assertTrue(peer2.syncedPeers.contains(3));
        assertTrue(peer3.syncedPeers.contains(2));
        assertFalse(peer2.syncedPeers.contains(4));
        assertFalse(peer4.syncedPeers.contains(2));

        // Read completes via mutually synchronized subset {2, 3}
        assertTrue(coordinator.isReadCompleted());
    }

    @Test
    public void testPairwiseMutualSyncArrivalOrderIndependence()
    {
        int[] summaryNodes = new int[]{ 2, 3, 4 };

        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 10 }, new int[]{});
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 6 }, new int[]{ 7, 10 });
        MutationSummary peer3Summary = summary(LOG_1, new int[]{ 1, 8 }, new int[]{ 9, 10 });
        MutationSummary peer4Summary = summary(LOG_1, new int[]{ 1, 9 }, new int[]{ 10, 10 });

        // Arrival sequence A: local summary after all remote summaries in reverse order
        ReadReconciliations.Coordinator coordA =
            new ReadReconciliations.Coordinator(new TrackedRead.Id(LOCAL_NODE_ID, 7004L), LOCAL_NODE_ID, summaryNodes, 2);
        coordA.acceptRemoteSummary(peer4Summary, 4, true);
        coordA.acceptRemoteSummary(peer3Summary, 3, true);
        coordA.acceptRemoteSummary(peer2Summary, 2, true);
        coordA.setLocalSummaryForTesting(localSummary);
        for (int offset = 7; offset <= 10; offset++)
            coordA.acceptMutation(new ShortMutationId(LOG_1, offset));
        assertTrue(coordA.isReadCompleted());

        // Arrival sequence B: local summary first, remote summaries interleaved
        ReadReconciliations.Coordinator coordB =
            new ReadReconciliations.Coordinator(new TrackedRead.Id(LOCAL_NODE_ID, 7005L), LOCAL_NODE_ID, summaryNodes, 2);
        coordB.setLocalSummaryForTesting(localSummary);
        coordB.acceptRemoteSummary(peer3Summary, 3, true);
        coordB.acceptRemoteSummary(peer2Summary, 2, true);
        for (int offset = 7; offset <= 10; offset++)
            coordB.acceptMutation(new ShortMutationId(LOG_1, offset));
        assertTrue(coordB.isReadCompleted());
        coordB.acceptRemoteSummary(peer4Summary, 4, true);

        // Assert pairwise sync maps match between both arrival sequences
        for (int node : summaryNodes)
        {
            assertEquals(coordA.getPeerState(node).syncedPeers, coordB.getPeerState(node).syncedPeers);
        }
    }

    @Test
    public void testPairwiseAsymmetricSyncRejectsQuorumUntilMutualAckReceived()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 7006L);
        int[] summaryNodes = new int[]{ 2, 3 };

        // Require dataNode plus 2 summary nodes
        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, LOCAL_NODE_ID, summaryNodes, 2);

        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 30 }, new int[]{});

        // Peer 2 has reconciled [1, 20] and unreconciled [21, 25]
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 20 }, new int[]{ 21, 25 });
        // Peer 3 has reconciled [1, 10] and unreconciled [11, 15]
        MutationSummary peer3Summary = summary(LOG_1, new int[]{ 1, 10 }, new int[]{ 11, 15 });

        coordinator.setLocalSummaryForTesting(localSummary);
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);
        coordinator.acceptRemoteSummary(peer3Summary, 3, true);

        // Complete local pulls for peer 2 and peer 3 unreconciled mutations
        for (int offset = 11; offset <= 15; offset++)
            coordinator.acceptMutation(new ShortMutationId(LOG_1, offset));
        for (int offset = 21; offset <= 25; offset++)
            coordinator.acceptMutation(new ShortMutationId(LOG_1, offset));

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        ReadReconciliations.Coordinator.PeerState peer3 = coordinator.getPeerState(3);

        // Peer 2 contains all of Peer 3's unreconciled [11, 15] in its reconciled set [1, 20]
        assertTrue(peer2.syncedPeers.contains(3));
        // Peer 3 does not contain Peer 2's unreconciled [21, 25]
        assertFalse(peer3.syncedPeers.contains(2));

        // Read cannot complete because sync is one-way (asymmetric), not mutual
        assertFalse(coordinator.isReadCompleted());

        // Peer 3 sends sync ack for Peer 2
        coordinator.acceptSyncAck(3, 2);
        assertTrue(peer3.syncedPeers.contains(2));

        // Read completes now that candidate quorum {2, 3} is mutually synchronized
        assertTrue(coordinator.isReadCompleted());
    }

    @Test
    public void testIntermediateMutationDoesNotTriggerQuorumCheck()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 4001L);
        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, LOCAL_NODE_ID, new int[]{ 2, 3 }, 2);

        MutationSummary localSummary = summary(LOG_1, new int[]{ 1, 15 }, new int[]{});
        MutationSummary peer2Summary = summary(LOG_1, new int[]{ 1, 5 }, new int[]{ 10, 12 });
        MutationSummary peer3Summary = summary(LOG_1, new int[]{ 1, 15 }, new int[]{});

        coordinator.setLocalSummaryForTesting(localSummary);
        // Peer 3 is in sync with 0 missing mutations, so Peer 3 is ready
        coordinator.acceptRemoteSummary(peer3Summary, 3, true);

        AtomicInteger quorumChecks = new AtomicInteger();
        coordinator.setQuorumCheckerForTesting(endpoints -> {
            quorumChecks.incrementAndGet();
            return endpoints.size() >= 3;
        });

        // Peer 2 has pending mutations [10, 12]
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);

        quorumChecks.set(0);

        // First intermediate mutation delivery does not transition peer to empty; quorum check must not run
        assertFalse(coordinator.acceptMutation(new ShortMutationId(LOG_1, 10)));
        assertEquals(0, quorumChecks.get());
        assertFalse(coordinator.isReadCompleted());

        // Second intermediate mutation delivery does not transition peer to empty; quorum check must not run
        assertFalse(coordinator.acceptMutation(new ShortMutationId(LOG_1, 11)));
        assertEquals(0, quorumChecks.get());
        assertFalse(coordinator.isReadCompleted());

        // Final mutation delivery transitions peer to empty; quorum check runs and completes read
        assertTrue(coordinator.acceptMutation(new ShortMutationId(LOG_1, 12)));
        assertTrue(quorumChecks.get() > 0);
        assertTrue(coordinator.isReadCompleted());
    }

    @Test
    public void testCompletedMutationsRangeIntervalDeduplication()
    {
        TrackedRead.Id readId = new TrackedRead.Id(LOCAL_NODE_ID, 4002L);
        ReadReconciliations.Coordinator coordinator =
            new ReadReconciliations.Coordinator(readId, LOCAL_NODE_ID, new int[]{ 2, 3 });

        MutationSummary peer2Summary = summary(LOG_2, new int[]{}, new int[]{ 10, 20 });
        coordinator.acceptRemoteSummary(peer2Summary, 2, true);

        for (int offset = 10; offset <= 20; offset++)
            coordinator.acceptMutation(new ShortMutationId(LOG_2, offset));

        ReadReconciliations.Coordinator.PeerState peer2 = coordinator.getPeerState(2);
        assertTrue(peer2.pendingLocalMutations.isEmpty());

        // Peer 3's summary arrives covering the full range [10, 20]
        MutationSummary peer3Summary = summary(LOG_2, new int[]{}, new int[]{ 10, 20 });
        coordinator.acceptRemoteSummary(peer3Summary, 3, true);

        ReadReconciliations.Coordinator.PeerState peer3 = coordinator.getPeerState(3);
        assertNotNull(peer3);
        assertTrue(peer3.pendingLocalMutations.isEmpty());
    }

    @Test
    public void testForReadIgnoresEndpointsOutsidePlan()
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        SimpleStrategy strategy = new SimpleStrategy("ks", Map.of("replication_factor", "3"), ReplicationType.tracked);
        ReadReconciliations.QuorumChecker checker = forRead(strategy, ConsistencyLevel.QUORUM, local, NODE_2, NODE_3);

        assertFalse(checker.isSufficient(candidates(local, NODE_4)));
        assertTrue(checker.isSufficient(candidates(local, NODE_2)));
    }

    @Test
    public void testForReadEachQuorumIgnoresEndpointsInUnknownDc()
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy("ks", Map.of(LOCAL_DC, "3"), ReplicationType.tracked);
        ReadReconciliations.QuorumChecker checker = forRead(strategy, ConsistencyLevel.EACH_QUORUM, local, NODE_2, NODE_3);

        assertTrue(checker.isSufficient(candidates(local, NODE_2, REMOTE_DC_NODE)));
        assertFalse(checker.isSufficient(candidates(local, REMOTE_DC_NODE)));
    }

    @Test
    public void testForReadLocalSerialCountsOnlyLocalDc()
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy("ks", Map.of(LOCAL_DC, "3", REMOTE_DC, "3"), ReplicationType.tracked);
        ReadReconciliations.QuorumChecker checker = forRead(strategy, ConsistencyLevel.LOCAL_SERIAL, local, NODE_2, NODE_3, REMOTE_DC_NODE);

        assertFalse(checker.isSufficient(candidates(local, REMOTE_DC_NODE)));
        assertTrue(checker.isSufficient(candidates(local, NODE_2)));
    }

    private static ReadReconciliations.QuorumChecker forRead(AbstractReplicationStrategy strategy,
                                                             ConsistencyLevel consistencyLevel,
                                                             InetAddressAndPort... planReplicas)
    {
        Replica[] replicas = new Replica[planReplicas.length];
        for (int i = 0; i < planReplicas.length; i++)
            replicas[i] = Replica.fullReplica(planReplicas[i], FULL_RANGE);
        EndpointsForToken endpoints = EndpointsForToken.of(FULL_RANGE.right, replicas);
        ReplicaPlan.ForTokenRead plan = new ReplicaPlan.ForTokenRead(null, strategy, consistencyLevel, endpoints, endpoints, endpoints,
                                                                     null, null, ClusterMetadata.current().epoch);
        return ReadReconciliations.QuorumChecker.forRead(plan);
    }

    private static EndpointsForRange candidates(InetAddressAndPort... endpoints)
    {
        Replica[] replicas = new Replica[endpoints.length];
        for (int i = 0; i < endpoints.length; i++)
            replicas[i] = Replica.fullReplica(endpoints[i], FULL_RANGE);
        return EndpointsForRange.of(replicas);
    }
}
