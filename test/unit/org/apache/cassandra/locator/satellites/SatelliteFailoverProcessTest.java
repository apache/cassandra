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
package org.apache.cassandra.locator.satellites;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SatelliteReplicationStrategy;
import org.apache.cassandra.locator.SatelliteReplicationStrategyTestBase;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.AdvanceSatelliteFailoverState;
import org.apache.cassandra.tcm.transformations.AdvanceSatelliteFailoverState.TargetState;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SatelliteFailoverProcessTest extends SatelliteReplicationStrategyTestBase
{
    private ExecutorPlus executor;

    private static InetAddressAndPort localEndpoint()
    {
        try { return InetAddressAndPort.getByName("10.0.0.10"); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    @After
    public void shutdownExecutor()
    {
        if (executor != null)
        {
            executor.shutdown();
            executor = null;
        }
    }

    private ExecutorPlus getExecutor()
    {
        if (executor == null)
            executor = executorFactory().pooled("test-failover", 2);
        return executor;
    }

    private List<Range<Token>> fullRing()
    {
        Token min = Murmur3Partitioner.instance.getMinimumToken();
        return List.of(new Range<>(min, min));
    }

    private List<Range<Token>> singleRange(long left, long right)
    {
        return List.of(new Range<>(new LongToken(left), new LongToken(right)));
    }

    @Test
    public void testCreateReturnsNullWhenNoActiveTransfer() throws Exception
    {
        createDualDCKeyspace("dc1");
        ClusterMetadata metadata = ClusterMetadata.current();

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, respondWithEpoch(metadata.epoch),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        assertNull("Should return null when no active transfer", process);
    }

    @Test
    public void testCreateSucceedsWithActiveTransfer() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        ClusterMetadata metadata = ClusterMetadata.current();

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, respondWithEpoch(metadata.epoch),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        assertNotNull("Should create process when transfer is active", process);
    }

    @Test
    public void testForceAckOnlyAdvancesToTransition() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        ClusterMetadata metadata = ClusterMetadata.current();

        // Verify we're in TRANSITION_ACK
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        SatelliteFailover.Info info = strategy.getFailoverInfo(metadata);
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, info.stateForToken(new LongToken(150)));

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, failAllMessages(),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        // force + ackOnly: should advance to TRANSITION without gate evaluation
        Future<?> result = process.start(getExecutor(), true, false, true);
        result.get();

        // Verify ranges advanced to TRANSITION
        ClusterMetadata updated = ClusterMetadata.current();
        SatelliteFailover.Info updatedInfo = getSRS(DUAL_DC_KEYSPACE).getFailoverInfo(updated);
        assertEquals(SatelliteFailover.State.TRANSITION, updatedInfo.stateForToken(new LongToken(150)));
    }

    @Test
    public void testBarrierOnlySkipsTransitionAckRanges() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        ClusterMetadata metadata = ClusterMetadata.current();

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, respondWithEpoch(metadata.epoch),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        // barrierOnly with all ranges in TRANSITION_ACK: nothing should happen
        Future<?> result = process.start(getExecutor(), false, true, false);
        result.get();

        // Ranges should still be in TRANSITION_ACK (nothing processed)
        ClusterMetadata updated = ClusterMetadata.current();
        SatelliteFailover.Info updatedInfo = getSRS(DUAL_DC_KEYSPACE).getFailoverInfo(updated);
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, updatedInfo.stateForToken(new LongToken(150)));
    }

    @Test
    public void testEpochAckFailurePreventsAdvancement() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        ClusterMetadata metadata = ClusterMetadata.current();

        // All endpoints respond with stale epoch
        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, respondWithEpoch(Epoch.EMPTY),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        Future<?> result = process.start(getExecutor(), true, false, false);

        try
        {
            result.get();
            fail("Should have failed with stale epochs");
        }
        catch (Exception e)
        {
            // expected -- epoch ack not met
        }

        // Ranges should still be in TRANSITION_ACK
        ClusterMetadata updated = ClusterMetadata.current();
        SatelliteFailover.Info updatedInfo = getSRS(DUAL_DC_KEYSPACE).getFailoverInfo(updated);
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, updatedInfo.stateForToken(new LongToken(150)));
    }

    @Test
    public void testEpochAckSucceedsWithQoQMet() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        // Make one endpoint in dc1 stale, QoQ should still be met (RF=3, quorum=2)
        Token token = new LongToken(150);
        Set<InetAddressAndPort> staleEndpoints = new HashSet<>();
        Set<InetAddressAndPort> dc1Endpoints = metadata.directory.datacenterEndpoints("dc1");
        staleEndpoints.add(strategy.calculateNaturalReplicas(token, metadata)
                                   .stream()
                                   .filter(r -> dc1Endpoints.contains(r.endpoint()))
                                   .findFirst()
                                   .orElseThrow()
                                   .endpoint());

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance,
            respondWithEpochExcept(metadata.epoch, Epoch.EMPTY, staleEndpoints),
            metadata, DUAL_DC_KEYSPACE, localEndpoint());

        // ackOnly + not force: epoch ack should pass but paxos repair will fail in unit test
        // context. The epoch ack passing means we get past that step.
        Future<?> result = process.start(getExecutor(), true, false, false);

        try
        {
            result.get(10, TimeUnit.SECONDS);
            // If it succeeds, epoch ack AND paxos repair both passed (unlikely in unit test)
        }
        catch (TimeoutException e)
        {
            // Expected -- paxos repair hangs in unit test context (no real messaging).
            // The fact that we got past epoch ack without failure proves it passed.
        }
        catch (Exception e)
        {
            // Expected -- paxos repair will fail in unit test context.
            // But the failure should NOT be from epoch ack.
            assertFalse("Failure should not be from epoch ack",
                         e.getMessage() != null && e.getMessage().contains("Epoch query failed"));
        }
    }

    // ========== Partially advanced range handling ==========

    /**
     * A range whose state has been advanced out from under us in its entirety has had the work of the
     * TRANSITION_ACK steps done for it already, so those steps are skipped. All messaging fails here, so if
     * anything were attempted the epoch check would fail the future.
     */
    @Test
    public void testFullyAdvancedRangeIsSkipped() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Snapshot metadata while everything is still TRANSITION_ACK, then advance the whole ring behind our back
        ClusterMetadata stale = ClusterMetadata.current();
        advanceOutOfBand(fullRing(), TargetState.TRANSITION);

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            fullRing(), SharedContext.Global.instance, failAllMessages(),
            stale, DUAL_DC_KEYSPACE, localEndpoint());

        process.start(getExecutor(), true, false, false).get();

        SatelliteFailover.Info info = getSRS(DUAL_DC_KEYSPACE).getFailoverInfo(ClusterMetadata.current());
        assertEquals(SatelliteFailover.State.TRANSITION, info.stateForToken(new LongToken(150)));
    }

    /**
     * A range that has been only partially advanced out from under us still needs its TRANSITION_ACK steps run
     * for the part that hasn't advanced. Here the upper part of the range (containing range.right, which used to
     * be the only token consulted) is advanced, and the lower part is not.
     */
    @Test
    public void testPartiallyAdvancedRangeIsNotSkipped() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        ClusterMetadata stale = ClusterMetadata.current();
        advanceOutOfBand(singleRange(150, 200), TargetState.TRANSITION);

        SatelliteFailoverProcess process = SatelliteFailoverProcess.create(
            singleRange(100, 200), SharedContext.Global.instance, failAllMessages(),
            stale, DUAL_DC_KEYSPACE, localEndpoint());

        try
        {
            process.start(getExecutor(), true, false, false).get();
            fail("Epoch check should have run for the sub-range that has not advanced");
        }
        catch (ExecutionException e)
        {
            // expected -- all messaging fails, so the epoch check fails
        }

        // The un-advanced sub-range must not have been advanced without its gates being met
        SatelliteFailover.Info info = getSRS(DUAL_DC_KEYSPACE).getFailoverInfo(ClusterMetadata.current());
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, info.stateForToken(new LongToken(120)));
        assertEquals(SatelliteFailover.State.TRANSITION, info.stateForToken(new LongToken(180)));
    }

    /**
     * A range behind the state its step expects would mean the range regressed, which monotonic advancement
     * forbids. It's reported through the returned future rather than thrown from start(), so a single bad range
     * doesn't abandon the pipelines of the ranges we haven't started yet.
     */
    @Test
    public void testRangeBehindExpectedStateFailsFuture() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        ClusterMetadata metadata = ClusterMetadata.current();
        // Claim the range is at TRANSITION while cluster metadata still has it at TRANSITION_ACK
        KeyspaceFailoverState regressed = metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE)
                                                                        .withRangesTransitioning(normalized(singleRange(100, 200)));

        SatelliteFailoverProcess process = new SatelliteFailoverProcess(singleRange(100, 200),
                                                                       SharedContext.Global.instance,
                                                                       failAllMessages(),
                                                                       Keyspace.open(DUAL_DC_KEYSPACE),
                                                                       getSRS(DUAL_DC_KEYSPACE),
                                                                       regressed);

        Future<?> result = process.start(getExecutor(), false, true, false);
        try
        {
            result.get();
            fail("Should have failed for a range behind the expected state");
        }
        catch (ExecutionException e)
        {
            assertTrue("Unexpected cause: " + e.getCause(), e.getCause() instanceof IllegalStateException);
            assertTrue("Unexpected message: " + e.getCause().getMessage(),
                       e.getCause().getMessage().contains("expected at least TRANSITION"));
        }

        SatelliteFailover.Info info = getSRS(DUAL_DC_KEYSPACE).getFailoverInfo(ClusterMetadata.current());
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, info.stateForToken(new LongToken(150)));
    }

    // ========== Plan preconditions ==========

    @Test
    public void testBarrierPlanAcceptsPartiallyAdvancedRange() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        advanceOutOfBand(singleRange(100, 200), TargetState.TRANSITION);
        advanceOutOfBand(singleRange(150, 200), TargetState.NORMAL);

        // (100, 150] is still TRANSITION, (150, 200] has moved on to NORMAL
        ClusterMetadata metadata = ClusterMetadata.current();
        assertNotNull(getSRS(DUAL_DC_KEYSPACE).planForFailoverBarrier(metadata,
                                                                     Keyspace.open(DUAL_DC_KEYSPACE),
                                                                     singleRange(100, 200).get(0)));
    }

    @Test
    public void testBarrierPlanRejectsRangeBehindTransition() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        advanceOutOfBand(singleRange(150, 200), TargetState.TRANSITION);

        // (100, 150] is still TRANSITION_ACK, so a barrier may not be planned for (100, 200]
        ClusterMetadata metadata = ClusterMetadata.current();
        try
        {
            getSRS(DUAL_DC_KEYSPACE).planForFailoverBarrier(metadata,
                                                            Keyspace.open(DUAL_DC_KEYSPACE),
                                                            singleRange(100, 200).get(0));
            fail("Should have rejected a range behind TRANSITION");
        }
        catch (IllegalStateException e)
        {
            // expected
        }
    }

    @Test
    public void testFailoverPlansRejectCompletedTransfer() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");
        advanceOutOfBand(fullRing(), TargetState.TRANSITION);
        advanceOutOfBand(fullRing(), TargetState.NORMAL);

        // The transfer is complete and its state removed, so there is no fromDC to coordinate against
        ClusterMetadata metadata = ClusterMetadata.current();
        assertNull(metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE));
        Keyspace ks = Keyspace.open(DUAL_DC_KEYSPACE);
        Range<Token> range = singleRange(100, 200).get(0);

        for (Runnable plan : List.<Runnable>of(() -> getSRS(DUAL_DC_KEYSPACE).planForFailoverBarrier(metadata, ks, range),
                                              () -> getSRS(DUAL_DC_KEYSPACE).planForFailoverEpochCheck(metadata, ks, range),
                                              () -> getSRS(DUAL_DC_KEYSPACE).planForFailoverPaxosRepair(metadata, ks, range)))
        {
            try
            {
                plan.run();
                fail("Should have rejected a completed transfer");
            }
            catch (IllegalStateException e)
            {
                assertEquals("No active failover transfer", e.getMessage());
            }
        }
    }

    // ========== Test Helpers ==========

    private static NormalizedRanges<Token> normalized(List<Range<Token>> ranges)
    {
        return NormalizedRanges.normalizedRanges(ranges);
    }

    private static void advanceOutOfBand(List<Range<Token>> ranges, TargetState target) throws Exception
    {
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(DUAL_DC_KEYSPACE, normalized(ranges), target));
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

    @SuppressWarnings("unchecked")
    private static MessageDelivery respondWithEpoch(Epoch responseEpoch)
    {
        return respondWithEpochExcept(responseEpoch, responseEpoch, Collections.emptySet());
    }

    @SuppressWarnings("unchecked")
    private static MessageDelivery respondWithEpochExcept(Epoch defaultEpoch,
                                                          Epoch exceptEpoch,
                                                          Set<InetAddressAndPort> exceptEndpoints)
    {
        return new MessageDelivery()
        {
            @Override
            public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
            {
                Epoch epoch = exceptEndpoints.contains(to) ? exceptEpoch : defaultEpoch;
                Message<Epoch> reply = Message.internalResponse(Verb.TCM_NOTIFY_RSP, epoch).withFrom(to);
                ((RequestCallback<Epoch>) cb).onResponse(reply);
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
            {
                sendWithCallback(message, to, cb);
            }

            @Override
            public <V> void respond(V response, Message<?> message)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static MessageDelivery failAllMessages()
    {
        return new MessageDelivery()
        {
            @Override
            public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
            {
                cb.onFailure(to, RequestFailure.UNKNOWN);
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
            {
                sendWithCallback(message, to, cb);
            }

            @Override
            public <V> void respond(V response, Message<?> message)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
