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
package org.apache.cassandra.service.paxos;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.PaxosCommitPropertyTest.TestableCommit;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.service.paxos.Commit.Agreed;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for PaxosCommit's augmented commit composition logic.
 *
 * Verifies that onDone fires correctly when paxos consensus is combined with an additional
 * commit future (from the replication strategy, e.g. satellite DC writes for SRS).
 * Either side failing should cause immediate completion with failure.
 */
public class SatellitePaxosCommitTest
{
    private static final String KEYSPACE = "spc_test";

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(3),
                                    SchemaLoader.standardCFMD(KEYSPACE, "Standard"));
    }

    private static TestableCommit<Consumer<PaxosCommit.Status>> createHandler(EndpointsForToken replicas,
                                                                              int required,
                                                                              AtomicReference<PaxosCommit.Status> statusCapture)
    {
        Keyspace ks = Keyspace.open(KEYSPACE);
        TableMetadata table = ks.getColumnFamilyStores().iterator().next().metadata();
        DecoratedKey key = table.partitioner.decorateKey(ByteBufferUtil.bytes(0));
        Agreed commit = new Agreed(Ballot.none(), PartitionUpdate.emptyUpdate(table, key));
        return new TestableCommit<>(commit, replicas, required,
                                    ConsistencyLevel.QUORUM, statusCapture::set,
                                    Collections.singleton(replicas.get(0).endpoint()));
    }

    private static EndpointsForToken threeReplicas() throws Exception
    {
        InetAddressAndPort ep1 = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort ep2 = InetAddressAndPort.getByName("127.0.0.2");
        InetAddressAndPort ep3 = InetAddressAndPort.getByName("127.0.0.3");
        Token minToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        Token maxToken = DatabaseDescriptor.getPartitioner().getRandomToken();
        return EndpointsForToken.of(maxToken,
                                    Replica.fullReplica(ep1, minToken, maxToken),
                                    Replica.fullReplica(ep2, minToken, maxToken),
                                    Replica.fullReplica(ep3, minToken, maxToken));
    }

    private static void sendSuccess(PaxosCommit<?> handler, InetAddressAndPort from)
    {
        Message<NoPayload> msg = Message.builder(Verb.ECHO_REQ, noPayload)
                                        .from(from)
                                        .build();
        handler.onResponse(msg);
    }

    // ========================================
    // No augmented commit (default behavior)
    // ========================================

    @Test
    public void testNoAugmentedCommit_paxosQuorumFiresOnDone() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        sendSuccess(handler, replicas.get(0).endpoint());
        assertNull("Should not fire after 1 response", status.get());

        sendSuccess(handler, replicas.get(1).endpoint());
        assertNotNull("Should fire after quorum", status.get());
        assertTrue("Should be success", status.get().isSuccess());
    }

    // ========================================
    // Already-completed futures
    // ========================================

    @Test
    public void testCompletedSuccessFuture_paxosQuorumFiresOnDone() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        handler.setAugmentedCommitFuture(ImmediateFuture.success(null));

        sendSuccess(handler, replicas.get(0).endpoint());
        assertNull(status.get());

        sendSuccess(handler, replicas.get(1).endpoint());
        assertNotNull("Should fire after quorum (future already done)", status.get());
        assertTrue("Should be success", status.get().isSuccess());
    }

    @Test
    public void testCompletedFailureFuture_failsImmediately() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        AsyncPromise<Void> failed = new AsyncPromise<>();
        failed.tryFailure(new RuntimeException("satellite quorum not met"));
        handler.setAugmentedCommitFuture(failed);

        // Future already failed — onDone should fire immediately
        assertNotNull("Should fire immediately on failed future", status.get());
        assertFalse("Should report failure", status.get().isSuccess());
    }

    // ========================================
    // Paxos completes first
    // ========================================

    @Test
    public void testPaxosSucceedsFirst_defersUntilFutureSucceeds() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        AsyncPromise<Void> promise = new AsyncPromise<>();
        handler.setAugmentedCommitFuture(promise);

        sendSuccess(handler, replicas.get(0).endpoint());
        sendSuccess(handler, replicas.get(1).endpoint());
        assertNull("onDone should NOT fire yet (future pending)", status.get());

        promise.trySuccess(null);
        assertNotNull("onDone should fire after future resolves", status.get());
        assertTrue("Should be success", status.get().isSuccess());
    }

    @Test
    public void testPaxosSucceedsFirst_futureFailsCausesFailure() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        AsyncPromise<Void> promise = new AsyncPromise<>();
        handler.setAugmentedCommitFuture(promise);

        sendSuccess(handler, replicas.get(0).endpoint());
        sendSuccess(handler, replicas.get(1).endpoint());
        assertNull("onDone deferred", status.get());

        promise.tryFailure(new RuntimeException("satellite quorum not met"));
        assertNotNull("onDone should fire", status.get());
        assertFalse("Should report failure", status.get().isSuccess());
    }

    @Test
    public void testPaxosFailsFirst_failsImmediately() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        AsyncPromise<Void> promise = new AsyncPromise<>();
        handler.setAugmentedCommitFuture(promise);

        // Paxos fails (enough failures to make quorum impossible)
        handler.onFailure(replicas.get(0).endpoint(), RequestFailure.UNKNOWN);
        handler.onFailure(replicas.get(1).endpoint(), RequestFailure.UNKNOWN);

        // Paxos failure should fire onDone immediately without waiting for the future
        assertNotNull("onDone should fire immediately on paxos failure", status.get());
        assertFalse("Should report paxos failure", status.get().isSuccess());
    }

    // ========================================
    // Future completes first
    // ========================================

    @Test
    public void testFutureSucceedsFirst_defersUntilPaxosSucceeds() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        AsyncPromise<Void> promise = new AsyncPromise<>();
        handler.setAugmentedCommitFuture(promise);

        promise.trySuccess(null);
        assertNull("onDone should NOT fire yet (paxos not done)", status.get());

        sendSuccess(handler, replicas.get(0).endpoint());
        assertNull(status.get());
        sendSuccess(handler, replicas.get(1).endpoint());
        assertNotNull("onDone should fire after paxos quorum", status.get());
        assertTrue("Should be success", status.get().isSuccess());
    }

    @Test
    public void testFutureFailsFirst_failsImmediately() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        AsyncPromise<Void> promise = new AsyncPromise<>();
        handler.setAugmentedCommitFuture(promise);

        promise.tryFailure(new RuntimeException("satellite quorum not met"));

        // Future failure should fire onDone immediately without waiting for paxos
        assertNotNull("onDone should fire immediately on future failure", status.get());
        assertFalse("Should report failure", status.get().isSuccess());
    }

    // ========================================
    // Null future (no-op)
    // ========================================

    @Test
    public void testNullFuture_behavesLikeNoAugmentedCommit() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        handler.setAugmentedCommitFuture(null);

        sendSuccess(handler, replicas.get(0).endpoint());
        assertNull(status.get());

        sendSuccess(handler, replicas.get(1).endpoint());
        assertNotNull("Should fire after quorum", status.get());
        assertTrue("Should be success", status.get().isSuccess());
    }

    /**
     * The callback registered by setAugmentedCommitFuture reads the augmentedCommit field, so setting a second future
     * would leave two callbacks reporting completion to the same AugmentedCommit and break its single-report
     * invariant. PaxosCommit#start must call it exactly once per commit.
     */
    @Test
    public void testSecondAugmentedCommitFutureRejected() throws Exception
    {
        EndpointsForToken replicas = threeReplicas();
        AtomicReference<PaxosCommit.Status> status = new AtomicReference<>();
        PaxosCommit<?> handler = createHandler(replicas, 2, status);

        handler.setAugmentedCommitFuture(new AsyncPromise<>());
        try
        {
            handler.setAugmentedCommitFuture(new AsyncPromise<>());
            fail("Setting a second augmented commit future should be rejected");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("already set"));
        }
    }
}
