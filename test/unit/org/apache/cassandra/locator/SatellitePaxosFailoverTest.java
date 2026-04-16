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
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.SatellitePaxosParticipants;
import org.apache.cassandra.service.reads.tracked.TrackedRead;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.transformations.AdvanceSatelliteFailoverState;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SatellitePaxosFailoverTest extends SatelliteReplicationStrategyTestBase
{
    private static final LongToken TOKEN = new LongToken(150);

    @Before
    public void registerLocalNode() throws Exception
    {
        // Register the local broadcast address in dc1 so shouldRejectPaxos can resolve the local DC
        InetAddress localAddr = InetAddress.getByName("127.0.0.1");
        DatabaseDescriptor.setBroadcastAddress(localAddr);
        InetAddressAndPort localEndpoint = InetAddressAndPort.getByAddress(localAddr);
        ClusterMetadataTestHelper.register(localEndpoint, "dc1", "rack1");
    }

    @After
    public void clearSinks()
    {
        MessagingService.instance().outboundSink.clear();
    }

    @Test
    public void testShouldRejectPaxosReturnsTrueDuringTransitionAck() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        assertTrue("Should reject paxos during TRANSITION_ACK", strategy.shouldRejectPaxos(TOKEN));
    }

    @Test
    public void testShouldRejectPaxosReturnsTrueWhenNotInPrimaryDC() throws Exception
    {
        // Local node is in dc1, but primary is dc2 — should reject
        createDualDCKeyspace("dc2");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        assertTrue("Should reject paxos when local node is not in primary DC", strategy.shouldRejectPaxos(TOKEN));
    }

    @Test
    public void testShouldRejectPaxosReturnsFalseInNormalState() throws Exception
    {
        // Local node is in dc1 and dc1 is primary — should allow
        createDualDCKeyspace("dc1");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        assertFalse("Should not reject paxos in NORMAL state when in primary DC", strategy.shouldRejectPaxos(TOKEN));
    }

    @Test
    public void testShouldRejectPaxosReturnsFalseDuringTransition() throws Exception
    {
        // Local node is in dc1 and dc1 is the new primary during TRANSITION — should allow
        createDualDCKeyspace("dc2");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc1");
        // Advance to TRANSITION
        Token min = DatabaseDescriptor.getPartitioner().getMinimumToken();
        NormalizedRanges<Token> fullRange = NormalizedRanges.normalizedRanges(
            Collections.singleton(new Range<>(min, min)));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));

        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        assertFalse("Should not reject paxos during TRANSITION when in primary DC", strategy.shouldRejectPaxos(TOKEN));
    }

    private TableMetadata tableMetadata(String keyspace)
    {
        return TableMetadata.builder(keyspace, "test_table")
                            .addPartitionKeyColumn("key", AsciiType.instance)
                            .build();
    }

    @Test
    public void testPaxosParticipantsRejectedDuringTransitionAck() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        try
        {
            strategy.paxosParticipants(ClusterMetadata.current(), tableMetadata(DUAL_DC_KEYSPACE),
                                       TOKEN, ConsistencyLevel.SERIAL, r -> true);
            fail("paxosParticipants should throw UnavailableException during TRANSITION_ACK");
        }
        catch (UnavailableException e)
        {
            // expected
        }
    }

    @Test
    public void testPaxosParticipantsAllowedInNormalState() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        Paxos.Participants participants = strategy.paxosParticipants(
            ClusterMetadata.current(), tableMetadata(DUAL_DC_KEYSPACE),
            TOKEN, ConsistencyLevel.SERIAL, r -> true);
        assertNotNull(participants);
    }

    @Test
    public void testSendPaxosCommitMutationsRejectedDuringTransitionAck() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        TableMetadata table = tableMetadata(DUAL_DC_KEYSPACE);
        DecoratedKey key = table.partitioner.decorateKey(bytes("test_key"));
        PartitionUpdate update = PartitionUpdate.emptyUpdate(table, key);
        Commit.Agreed commit = new Commit.Agreed(Ballot.none(), update);

        try
        {
            strategy.sendPaxosCommitMutations(commit, false);
            fail("sendPaxosCommitMutations should throw UnavailableException during TRANSITION_ACK");
        }
        catch (UnavailableException e)
        {
            // expected
        }
    }

    @Test
    public void testSendPaxosCommitMutationsFailsWhenSatelliteRequestsFail() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        // Mark all non-primary (satellite/secondary) endpoints alive so they are contacted with a callback,
        // rather than being pre-marked as failed via the downEndpoints path.
        markAllEndpointsAlive();

        // Capture and swallow the outbound satellite commit requests so no real network I/O happens; we drive
        // the failure ourselves via callback expiration below.
        List<MessageCapture> captured = new CopyOnWriteArrayList<>();
        MessagingService.instance().outboundSink.add((message, to) -> {
            if (message.verb() == Verb.PAXOS2_COMMIT_REMOTE_REQ)
                captured.add(new MessageCapture(message, to));
            return false;
        });

        Commit.Agreed commit = agreedCommit();

        Future<Void> future = strategy.sendPaxosCommitMutations(commit, false);

        // A quorum of satellite endpoints must have been contacted with a callback.
        assertFalse("Expected satellite commit requests to be sent", captured.isEmpty());
        assertFalse("Future should not complete before any responses/failures", future.isDone());

        // Simulate a request failure (timeout) for every satellite endpoint. This exercises the callback's
        // onFailure path; it only runs because invokeOnFailure() returns true.
        for (MessageCapture cap : captured)
            MessagingService.instance().callbacks.onExpired(cap.message, cap.to);

        assertTrue("Future should fail once satellite quorum is unreachable", future.awaitUninterruptibly(30, TimeUnit.SECONDS));
        assertTrue("Future should have failed", future.isDone() && !future.isSuccess());
        assertNotNull("Failure cause should be set", future.cause());
    }

    @Test
    public void testSendPaxosCommitMutationsSucceedsWhenSatelliteRequestsRespond() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        markAllEndpointsAlive();

        List<MessageCapture> captured = new CopyOnWriteArrayList<>();
        MessagingService.instance().outboundSink.add((message, to) -> {
            if (message.verb() == Verb.PAXOS2_COMMIT_REMOTE_REQ)
                captured.add(new MessageCapture(message, to));
            return false;
        });

        Commit.Agreed commit = agreedCommit();

        Future<Void> future = strategy.sendPaxosCommitMutations(commit, false);

        assertFalse("Expected satellite commit requests to be sent", captured.isEmpty());

        // Deliver a successful response for every satellite endpoint via the registered callback.
        for (MessageCapture cap : captured)
        {
            Message<NoPayload> response = Message.internalResponse(Verb.PAXOS2_COMMIT_REMOTE_RSP, NoPayload.noPayload)
                                                 .withFrom(cap.to);
            MessagingService.instance().callbacks.removeAndRespond(cap.message.id(), cap.to, response);
        }

        assertTrue("Future should complete once satellite quorum responds",
                   future.awaitUninterruptibly(30, TimeUnit.SECONDS));
        assertTrue("Future should have succeeded", future.isSuccess());
    }

    private Commit.Agreed agreedCommit()
    {
        TableMetadata table = tableMetadata(DUAL_DC_KEYSPACE);
        // Pin the key to TOKEN (150) so it maps to the configured ring ranges set up by the test base.
        DecoratedKey key = new BufferDecoratedKey(TOKEN, bytes("test_key"));
        PartitionUpdate update = PartitionUpdate.emptyUpdate(table, key);
        // A non-none mutation id is required by sendPaxosCommitMutations (it registers with mutation tracking).
        return new Commit.Agreed(Ballot.none(), update).withMutationId(new MutationId(1L, 1L));
    }

    private void markAllEndpointsAlive()
    {
        for (InetAddressAndPort endpoint : ClusterMetadata.current().directory.allAddresses())
            Gossiper.instance.initializeNodeUnsafe(endpoint, UUID.randomUUID(), 1);
    }

    private SatellitePaxosParticipants getParticipants(String keyspace) throws Exception
    {
        SatelliteReplicationStrategy strategy = getSRS(keyspace);
        ClusterMetadata metadata = ClusterMetadata.current();
        Paxos.Participants participants = strategy.paxosParticipants(
            metadata, tableMetadata(keyspace), TOKEN, ConsistencyLevel.SERIAL, r -> true);
        assertTrue("SRS should return SatellitePaxosParticipants",
                   participants instanceof SatellitePaxosParticipants);
        return (SatellitePaxosParticipants) participants;
    }

    @Test
    public void testPaxosParticipantsReturnsSatelliteEndpoints() throws Exception
    {
        // Dual DC with dc1 primary: satellite endpoints should include sat1 (dc1's satellite) and dc2 (other full DC)
        createDualDCKeyspace("dc1");
        SatellitePaxosParticipants spp = getParticipants(DUAL_DC_KEYSPACE);

        EndpointsForToken satelliteEndpoints = spp.getAdditionalSummaryEndpoints();
        ClusterMetadata metadata = ClusterMetadata.current();
        Set<String> dcs = replicaDCs(satelliteEndpoints, metadata);

        assertTrue("Should include sat1 (primary's satellite)", dcs.contains("sat1"));
        assertTrue("Should include dc2 (other full DC)", dcs.contains("dc2"));
        assertFalse("Should not include dc1 (primary DC)", dcs.contains("dc1"));
        assertFalse("Should not include sat2 (other DC's satellite)", dcs.contains("sat2"));
    }

    /**
     * {@link org.apache.cassandra.service.paxos.PaxosCommit} sends the commit to every endpoint in
     * {@code allLive} and looks each response up in {@code liveAndDown()}, so {@code allLive} must stay within it.
     * Satellite and secondary DC replicas reject paxos commits (see
     * {@link SatelliteReplicationStrategy#shouldRejectPaxos}), and those rejections would be counted against the
     * primary DC electorate and fail the commit. Those DCs receive the committed mutation via
     * {@link SatelliteReplicationStrategy#sendPaxosCommitMutations} instead.
     */
    private void assertPaxosCommitTargets(String keyspace, String primaryDC) throws Exception
    {
        SatellitePaxosParticipants spp = getParticipants(keyspace);
        ClusterMetadata metadata = ClusterMetadata.current();

        assertEquals("Commit should only be sent to the primary DC",
                     Collections.singleton(primaryDC), replicaDCs(spp.allLive(), metadata));

        Set<InetAddressAndPort> liveAndDown = spp.liveAndDown().endpoints();
        for (Replica replica : spp.allLive())
            assertTrue("Commit target " + replica + " is not in liveAndDown() " + liveAndDown,
                       liveAndDown.contains(replica.endpoint()));
    }

    @Test
    public void testPaxosCommitTargetsScopedToPrimaryDC() throws Exception
    {
        createDualDCKeyspace("dc1");
        assertPaxosCommitTargets(DUAL_DC_KEYSPACE, "dc1");
    }

    @Test
    public void testPaxosCommitTargetsFollowPrimaryDC() throws Exception
    {
        createDualDCKeyspace("dc2");
        assertPaxosCommitTargets(DUAL_DC_KEYSPACE, "dc2");
    }

    @Test
    public void testPaxosCommitTargetsSingleDC() throws Exception
    {
        createSingleDCKeyspace();
        assertPaxosCommitTargets(SINGLE_DC_KEYSPACE, "dc1");
    }

    @Test
    public void testPaxosParticipantsSingleDCHasSatelliteOnly() throws Exception
    {
        createSingleDCKeyspace();
        SatellitePaxosParticipants spp = getParticipants(SINGLE_DC_KEYSPACE);

        EndpointsForToken satelliteEndpoints = spp.getAdditionalSummaryEndpoints();
        ClusterMetadata metadata = ClusterMetadata.current();
        Set<String> dcs = replicaDCs(satelliteEndpoints, metadata);

        assertTrue("Should include sat1", dcs.contains("sat1"));
        assertEquals("Should only have sat1", 1, dcs.size());
    }

    @Test
    public void testAdditionalSummaryHostIdsMatchesSatelliteEndpoints() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatellitePaxosParticipants spp = getParticipants(DUAL_DC_KEYSPACE);

        ClusterMetadata metadata = ClusterMetadata.current();
        EndpointsForToken satelliteEndpoints = spp.getAdditionalSummaryEndpoints();
        int[] additionalIds = spp.additionalSummaryHostIds(metadata);

        assertEquals(satelliteEndpoints.size(), additionalIds.length);
        for (int i = 0; i < satelliteEndpoints.size(); i++)
        {
            int expectedId = metadata.directory.peerId(satelliteEndpoints.endpoint(i)).id();
            assertEquals(expectedId, additionalIds[i]);
        }
    }

    @Test
    public void testOnPrepareStartedSendsSummaryRequestToSatellites() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatellitePaxosParticipants spp = getParticipants(DUAL_DC_KEYSPACE);

        List<MessageCapture> captured = new CopyOnWriteArrayList<>();
        MessagingService.instance().outboundSink.add((message, to) -> {
            captured.add(new MessageCapture(message, to));
            return false;
        });

        TrackedRead.Id readId = new TrackedRead.Id(1, 100L);
        TableMetadata table = tableMetadata(DUAL_DC_KEYSPACE);
        SinglePartitionReadCommand readCommand = SinglePartitionReadCommand.fullPartitionRead(table, 0, ByteBufferUtil.bytes(0));

        spp.onPrepareStarted(readId, 42, new int[] { 1, 2, 3 }, readCommand);

        EndpointsForToken satelliteEndpoints = spp.getAdditionalSummaryEndpoints();
        assertEquals(satelliteEndpoints.size(), captured.size());

        Set<InetAddressAndPort> sentTo = captured.stream().map(c -> c.to).collect(Collectors.toSet());
        for (MessageCapture cap : captured)
            assertEquals(Verb.TRACKED_SUMMARY_REQ, cap.message.verb());
        for (int i = 0; i < satelliteEndpoints.size(); i++)
            assertTrue("Should send to satellite endpoint", sentTo.contains(satelliteEndpoints.endpoint(i)));
    }

    @Test
    public void testOnPrepareStartedNoOpWhenReadCommandNull() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatellitePaxosParticipants spp = getParticipants(DUAL_DC_KEYSPACE);

        List<MessageCapture> captured = new CopyOnWriteArrayList<>();
        MessagingService.instance().outboundSink.add((message, to) -> {
            captured.add(new MessageCapture(message, to));
            return false;
        });

        spp.onPrepareStarted(new TrackedRead.Id(1, 100L), 42, new int[] { 1, 2, 3 }, null);

        assertEquals(0, captured.size());
    }

    private void alterKeyspacePrimary(String keyspace, String newPrimary) throws Exception
    {
        String cql = "ALTER KEYSPACE " + keyspace + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '3/3', " +
                     "'primary': '" + newPrimary + "'" +
                     "} AND replication_type = 'tracked'";
        AlterSchemaStatement stmt = (AlterSchemaStatement) QueryProcessor.parseStatement(cql)
            .prepare(ClientState.forInternalCalls());
        ClusterMetadataTestHelper.commit(new AlterSchema(stmt));
    }

    private static class MessageCapture
    {
        final Message<?> message;
        final InetAddressAndPort to;

        MessageCapture(Message<?> message, InetAddressAndPort to)
        {
            this.message = message;
            this.to = to;
        }
    }
}
