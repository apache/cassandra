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

package org.apache.cassandra.distributed.test.tracking;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Utility methods for Paxos mutation tracking migration tests.
 *
 * Reduces boilerplate around deserializing messages, extracting MutationIds,
 * generating synthetic failure responses, stranding nodes at old TCM epochs, and
 * composing common cluster-level filter-and-count spy patterns.
 */
public class PaxosMigrationTestUtils
{
    private static final AtomicInteger KEYSPACE_COUNTER = new AtomicInteger();

    private PaxosMigrationTestUtils()
    {
    }

    // Must be called inside callsOnInstance() or runOnInstance() on the receiving node
    public static boolean messageHasMutationId(IMessage msg)
    {
        Message<?> deserialized = Instance.deserializeMessage(msg);
        return payloadHasMutationId(deserialized.payload);
    }

    // Must be called inside runOnInstance() on the receiving node
    public static void respondWithTimeout(IMessage msg)
    {
        Message<?> deserialized = Instance.deserializeMessage(msg);
        MessagingService.instance().respondWithFailure(RequestFailureReason.TIMEOUT, deserialized);
    }

    private static MutationId extractMutationIdFromPayload(Object payload)
    {
        if (payload instanceof Commit)
            return ((Commit) payload).mutation.id();

        if (payload instanceof Mutation)
            return ((Mutation) payload).id();

        try
        {
            for (Field f : payload.getClass().getDeclaredFields())
            {
                if (f.getName().equals("missingCommit") || f.getName().equals("mutation") || f.getName().equals("commit"))
                {
                    f.setAccessible(true);
                    Object val = f.get(payload);
                    if (val instanceof Commit)
                        return ((Commit) val).mutation.id();
                    if (val instanceof Mutation)
                        return ((Mutation) val).id();
                }
            }
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }

        return MutationId.none();
    }

    private static boolean payloadHasMutationId(Object payload)
    {
        return !extractMutationIdFromPayload(payload).isNone();
    }

    public static void awaitReplicationType(Cluster cluster, String keyspace, ReplicationType expected, int... nodes)
    {
        boolean expectTracked = expected == ReplicationType.tracked;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        for (int node : nodes)
        {
            while (System.nanoTime() < deadline)
            {
                boolean isTracked = cluster.get(node).callOnInstance(() ->
                    ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).params.replicationType.isTracked());
                if (isTracked == expectTracked)
                    break;
                Thread.yield();
            }
            boolean isTracked = cluster.get(node).callOnInstance(() ->
                ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).params.replicationType.isTracked());
            if (isTracked != expectTracked)
                throw new AssertionError("Node " + node + " did not see replicationType " +
                                         expected + " for " + keyspace + " within 30s");
        }
    }

    // --- Cluster builder helpers ---

    /**
     * Build a Cluster.Builder preconfigured for Paxos migration tests: NETWORK + GOSSIP, given
     * paxos_variant, long (10s) write/CAS contention timeouts. The caller is responsible for
     * calling .start() and wrapping in init() (which requires TestBaseImpl access), and for
     * invoking {@link #pauseHintsAndReconciler(Cluster)} after the cluster is live.
     */
    public static Cluster.Builder buildPaxosCluster(int nodes, String paxosVariant)
    {
        return Cluster.build(nodes)
                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                            .with(Feature.GOSSIP)
                                            .set("paxos_variant", paxosVariant)
                                            .set("write_request_timeout", "10000ms")
                                            .set("cas_contention_timeout", "10000ms"));
    }

    /**
     * Pause hint delivery and the regular-priority mutation tracking reconciler on every node in
     * the cluster. Intended to be called once in @BeforeClass after the cluster is started so that
     * hints and reconciliation traffic don't bleed into tests.
     */
    public static void pauseHintsAndReconciler(Cluster cluster)
    {
        cluster.forEach(instance -> instance.runOnInstance(() -> HintsService.instance.pauseDispatch()));
        cluster.forEach(instance -> instance.runOnInstance(() -> MutationTrackingService.instance().pauseActiveReconcilerRegularPriority()));
    }

    // --- Keyspace / schema helpers ---

    /**
     * Creates a fresh keyspace with unique name (prefix_N) using SimpleStrategy at the requested
     * RF and replication_type, plus a standard table {@code tbl (k int PRIMARY KEY, v int, v2 int)}
     * covering every field used across the paxos-migration test suite.
     *
     * @return the generated keyspace name.
     */
    public static String createKeyspace(Cluster cluster, String prefix, String replicationType, int rf)
    {
        String ks = prefix + '_' + KEYSPACE_COUNTER.incrementAndGet();
        cluster.schemaChange("CREATE KEYSPACE " + ks + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': " + rf + "} " +
                             "AND replication_type='" + replicationType + "'");
        cluster.schemaChange("CREATE TABLE " + ks + ".tbl (k int PRIMARY KEY, v int, v2 int)");
        return ks;
    }

    /** Convenience overload defaulting to RF=3. */
    public static String createKeyspace(Cluster cluster, String prefix, String replicationType)
    {
        return createKeyspace(cluster, prefix, replicationType, 3);
    }

    public static void awaitMigrationComplete(Cluster cluster, String keyspace)
    {
        String ks = keyspace;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true)
        {
            boolean migrating = cluster.get(1).callOnInstance(() ->
                ClusterMetadata.current().mutationTrackingMigrationState.isMigrating(ks));
            if (!migrating)
                return;
            if (System.nanoTime() >= deadline)
                throw new AssertionError("Migration did not complete within 30s for keyspace " + keyspace);
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
    }

    public static void alterReplicationType(Cluster cluster, String keyspace, String replicationType)
    {
        cluster.schemaChange("ALTER KEYSPACE " + keyspace + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                             "AND replication_type='" + replicationType + "'");
    }

    public static void alterReplicationTypeFrom(Cluster cluster,
                                                int coordinator,
                                                String keyspace,
                                                String replicationType,
                                                ConsistencyLevel cl)
    {
        cluster.coordinator(coordinator).execute("ALTER KEYSPACE " + keyspace + " WITH replication = " +
                                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                                 "AND replication_type='" + replicationType + "'",
                                                 cl);
    }

    // --- Assertion helpers ---

    public static void assertReplicasHaveValue(Cluster cluster, String keyspace, int key, Object value, int... nodes)
    {
        for (int node : nodes)
        {
            Util.spinAssertEquals(value,
                                 () -> {
                                     Object[][] r = cluster.get(node).executeInternal("SELECT v FROM " + keyspace + ".tbl WHERE k = " + key);
                                     return r.length == 1 ? r[0][0] : null;
                                 });
        }
    }

    public static void assertReplicaHasNoRow(Cluster cluster, String keyspace, int key, int node)
    {
        Object[][] nodeResult = cluster.get(node).executeInternal("SELECT v FROM " + keyspace + ".tbl WHERE k = " + key);
        assertEquals("Node " + node + " should have no row for k=" + key, 0, nodeResult.length);
    }

    public static void assertAllNodesSee(Cluster cluster, String keyspace, ReplicationType expected)
    {
        for (int i = 1; i <= cluster.size(); i++)
            assertNodeSees(cluster, i, keyspace, expected);
    }

    public static void assertNodeSees(Cluster cluster, int node, String keyspace, ReplicationType expected)
    {
        boolean expectTracked = expected == ReplicationType.tracked;
        boolean isTracked = cluster.get(node).callOnInstance(() ->
            ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).params.replicationType.isTracked());
        assertEquals("Node " + node + " should see replicationType " + expected + " for " + keyspace,
                     expectTracked, isTracked);
    }

    /**
     * Fail-fast assertion: if the partitioner or token allocation changes such that {@code key}
     * no longer maps to the expected replica set, every test that depends on that placement is
     * silently invalid. Validate explicitly up-front so a placement regression fails loudly
     * rather than via vacuous message counts.
     */
    public static void assertReplicasAreExactly(Cluster cluster, String keyspace, int key, int[] expectedReplicas)
    {
        String[] actualReplicas = cluster.get(1).applyOnInstance((String ks, Integer k) -> {
            Keyspace keyspaceObj = Keyspace.open(ks);
            Token token = keyspaceObj.getColumnFamilyStore("tbl").getPartitioner()
                .getToken(Int32Type.instance.decompose(k));
            return keyspaceObj.getReplicationStrategy()
                .calculateNaturalReplicas(token, ClusterMetadata.current())
                .endpoints().stream()
                .map(ep -> ep.getAddress().getHostAddress() + ":" + ep.getPort())
                .toArray(String[]::new);
        }, keyspace, key);

        String[] expected = new String[expectedReplicas.length];
        for (int i = 0; i < expectedReplicas.length; i++)
            expected[i] = cluster.get(expectedReplicas[i]).broadcastAddress().getAddress().getHostAddress()
                          + ":" + cluster.get(expectedReplicas[i]).broadcastAddress().getPort();

        Set<String> actualSet = new HashSet<>(Arrays.asList(actualReplicas));
        Set<String> expectedSet = new HashSet<>(Arrays.asList(expected));
        assertEquals("KEY=" + key + " placement assumption violated. Actual replicas: " + actualSet
                     + " Expected: " + expectedSet + " — tests that depend on this placement are invalid.",
                     expectedSet, actualSet);
    }

    /**
     * Recognize a CAS-related write exception by simple-name string comparison. Uses string
     * comparison because the exception may have been deserialized in a different classloader
     * (in-JVM dtest nodes use isolated classloaders) and {@code instanceof} would return false.
     */
    public static void assertCasException(Exception e)
    {
        String actual = e.getClass().getName();
        boolean recognized = actual.equals(CasWriteTimeoutException.class.getName())
                             || actual.equals(CasWriteUnknownResultException.class.getName())
                             || actual.equals(WriteTimeoutException.class.getName())
                             || actual.equals(WriteFailureException.class.getName());
        assertTrue("Expected a CAS-related write exception but got " + actual + ": " + e.getMessage(),
                   recognized);
    }

    public static void assertCasApplied(Object[][] result)
    {
        assertNotNull("CAS should return a result", result);
        assertEquals(1, result.length);
        assertTrue("CAS should be applied", (boolean) result[0][0]);
    }

    public static void assertCasNotApplied(Object[][] result)
    {
        assertNotNull("CAS should return a result", result);
        assertEquals(1, result.length);
        assertFalse("CAS should NOT be applied", (boolean) result[0][0]);
    }

    // --- Async CAS helpers ---

    /**
     * Execute a CAS on the given coordinator at SERIAL/QUORUM asynchronously. The returned
     * future completes with the result or completes exceptionally.
     */
    public static CompletableFuture<Object[][]> casAsync(Cluster cluster, int coordinator, String cql)
    {
        return CompletableFuture.supplyAsync(() ->
            cluster.coordinator(coordinator).execute(cql, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM));
    }

    /**
     * Execute a CAS expected to fail. The returned future completes with the raised Throwable
     * (or null if the CAS unexpectedly succeeded).
     */
    public static CompletableFuture<Throwable> casAsyncExpectingFailure(Cluster cluster, int coordinator, String cql)
    {
        return CompletableFuture.supplyAsync(() -> {
            try
            {
                cluster.coordinator(coordinator).execute(cql, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
                return null;
            }
            catch (Throwable t)
            {
                return t;
            }
        });
    }

    // --- EpochPin: hold TCM traffic so a node stays at its old epoch ---

    /**
     * Install filters that pin a node at its current TCM epoch by holding inbound
     * TCM_REPLICATION / TCM_NOTIFY_REQ messages targeted at the node, and outbound
     * TCM_FETCH_CMS_LOG_REQ / TCM_FETCH_PEER_LOG_REQ messages originating from it, until
     * {@link EpochPin#close()} releases the hold latch. Messages are held rather than
     * dropped so they eventually flow and the node catches up before subsequent tests — an
     * indefinite fetchLogFromPeerOrCMS stall would bleed into later tests.
     */
    public static EpochPin epochPin(Cluster cluster, int node)
    {
        return new EpochPin(cluster, node);
    }

    public static final class EpochPin implements AutoCloseable
    {
        private final AssertingLatch releaseTcm;
        private final IMessageFilters.Filter inboundFilter;
        private final IMessageFilters.Filter outboundFilter;

        private EpochPin(Cluster cluster, int node)
        {
            this.releaseTcm = new AssertingLatch("EpochPin release for node " + node);
            this.inboundFilter = cluster.filters()
                                        .inbound(true)
                                        .verbs(Verb.TCM_REPLICATION.id, Verb.TCM_NOTIFY_REQ.id)
                                        .to(node)
                                        .messagesMatching((from, to, msg) -> {
                                            releaseTcm.await();
                                            return false;
                                        }).drop();
            this.outboundFilter = cluster.filters()
                                         .inbound(true)
                                         .verbs(Verb.TCM_FETCH_CMS_LOG_REQ.id, Verb.TCM_FETCH_PEER_LOG_REQ.id)
                                         .from(node)
                                         .messagesMatching((from, to, msg) -> {
                                             releaseTcm.await();
                                             return false;
                                         }).drop();
        }

        @Override
        public void close()
        {
            // Release the hold so held messages flow through the filter. We deliberately do NOT
            // call .off() here; the filter remains installed so any late messages still flow
            // rather than being silently dropped. Callers typically reset filters via cluster.filters().reset()
            // in @After.
            releaseTcm.countDown();
        }

        public IMessageFilters.Filter inboundFilter()
        {
            return inboundFilter;
        }

        public IMessageFilters.Filter outboundFilter()
        {
            return outboundFilter;
        }
    }

    // --- MessageSpy: fluent wrapper over cluster filter + counter + latch boilerplate ---

    /**
     * Create a {@link Builder} that configures a spy (or dropper) over one or more message verbs
     * on the given cluster. Default direction is inbound. Returns a {@link MessageSpy} once
     * {@link Builder#start()} is called.
     */
    public static Builder on(Cluster cluster, Verb... verbs)
    {
        return new Builder(cluster, verbs);
    }

    /**
     * Fluent builder for {@link MessageSpy}. Configures direction, source/destination nodes,
     * mutation-id checking, expected message count, hold/release behavior, and whether messages
     * should be dropped or allowed through.
     */
    public static final class Builder
    {
        private final Cluster cluster;
        private final int[] verbIds;
        private int[] fromNodes;
        private int[] toNodes;
        private boolean inbound = true;
        private boolean checkMutationId = false;
        private int expect = 0;
        private boolean holdAll = false;
        private int holdFirst = 0;
        private boolean drop = false;
        private final List<IMessageFilters.Matcher> observers = new ArrayList<>();

        private Builder(Cluster cluster, Verb... verbs)
        {
            this.cluster = cluster;
            this.verbIds = new int[verbs.length];
            for (int i = 0; i < verbs.length; i++)
                this.verbIds[i] = verbs[i].id;
        }

        public Builder from(int... nodes)
        {
            this.fromNodes = nodes;
            return this;
        }

        public Builder to(int... nodes)
        {
            this.toNodes = nodes;
            return this;
        }

        /** Set the filter to apply on inbound (the default) or outbound traffic. */
        public Builder inbound()
        {
            this.inbound = true;
            return this;
        }

        public Builder inbound(boolean inbound)
        {
            this.inbound = inbound;
            return this;
        }

        /** Deserialize each matching message on its target node and count those carrying a mutation id. */
        public Builder checkMutationId()
        {
            this.checkMutationId = true;
            return this;
        }

        /** Arm an internal {@link AssertingLatch} so {@link MessageSpy#await()} waits for N matching messages. */
        public Builder expect(int count)
        {
            this.expect = count;
            return this;
        }

        /**
         * Hold every matching message on an internal latch until {@link MessageSpy#release()} is
         * called. Messages are still counted and (if configured) checked for a mutation id before
         * blocking.
         */
        public Builder holdAll()
        {
            this.holdAll = true;
            return this;
        }

        /** Hold only the first {@code n} matching messages; the rest flow through immediately. */
        public Builder holdFirst(int n)
        {
            this.holdFirst = n;
            return this;
        }

        /** Drop matching messages (don't deliver). Default is spy-only — messages flow through. */
        public Builder drop()
        {
            this.drop = true;
            return this;
        }

        /** Register an extra observer invoked on every matching message (after counting / holding). */
        public Builder onEach(IMessageFilters.Matcher observer)
        {
            this.observers.add(observer);
            return this;
        }

        public MessageSpy start()
        {
            return new MessageSpy(this);
        }
    }

    /**
     * A running message spy — counts matching messages, optionally holds them, optionally checks
     * for mutation ids, and fires an expectation latch once the target count is reached.
     *
     * Closing via {@link #close()} turns the underlying filter off and releases any held messages.
     */
    public static final class MessageSpy implements AutoCloseable
    {
        private final Cluster cluster;
        private final AtomicInteger total = new AtomicInteger();
        private final AtomicInteger withMutationId = new AtomicInteger();
        private final AtomicInteger held = new AtomicInteger();
        private final AtomicInteger passedThrough = new AtomicInteger();
        private final AssertingLatch deliveryLatch;
        private final AssertingLatch firstArrivalLatch;
        private final AssertingLatch holdLatch;
        private final IMessageFilters.Filter filter;
        private final boolean drop;
        private final int holdFirst;
        private final boolean holdAll;

        private MessageSpy(Builder b)
        {
            this.cluster = b.cluster;
            this.drop = b.drop;
            this.holdFirst = b.holdFirst;
            this.holdAll = b.holdAll;
            this.deliveryLatch = b.expect > 0 ? new AssertingLatch(b.expect, "MessageSpy delivery (expect=" + b.expect + ")")
                                              : null;
            this.firstArrivalLatch = new AssertingLatch("MessageSpy first arrival");
            this.holdLatch = (b.holdAll || b.holdFirst > 0) ? new AssertingLatch("MessageSpy hold release") : null;

            IMessageFilters.Builder fb = cluster.filters().inbound(b.inbound).verbs(b.verbIds);
            if (b.fromNodes != null)
                fb = fb.from(b.fromNodes);
            if (b.toNodes != null)
                fb = fb.to(b.toNodes);

            final boolean checkId = b.checkMutationId;
            final List<IMessageFilters.Matcher> observers = b.observers;
            this.filter = fb.messagesMatching((from, to, msg) -> {
                total.incrementAndGet();
                firstArrivalLatch.countDown();

                if (checkId)
                {
                    boolean hasId = cluster.get(to).callsOnInstance(() -> messageHasMutationId(msg)).call();
                    if (hasId)
                        withMutationId.incrementAndGet();
                }

                for (IMessageFilters.Matcher observer : observers)
                    observer.matches(from, to, msg);

                // Hold before signaling deliveryLatch so the held-message-count stays accurate
                // while the hold is still active.
                boolean shouldHold = holdAll || (holdFirst > 0 && total.get() <= holdFirst);
                if (shouldHold)
                {
                    held.incrementAndGet();
                    holdLatch.await();
                }
                else
                {
                    passedThrough.incrementAndGet();
                }

                if (deliveryLatch != null)
                    deliveryLatch.countDown();

                return drop;
            }).drop();
        }

        public int total()
        {
            return total.get();
        }

        public int withMutationId()
        {
            return withMutationId.get();
        }

        public int held()
        {
            return held.get();
        }

        public int passedThrough()
        {
            return passedThrough.get();
        }

        /** Block until the {@code expect(N)} count is reached. Throws if no expectation was set. */
        public void await()
        {
            if (deliveryLatch == null)
                throw new IllegalStateException("await() requires expect(N) to have been set at builder time");
            deliveryLatch.await();
        }

        /** Block until at least one matching message has arrived. */
        public void awaitFirstArrival()
        {
            firstArrivalLatch.await();
        }

        /** Release messages blocked by {@code holdAll()} / {@code holdFirst(N)}. */
        public void release()
        {
            if (holdLatch == null)
                throw new IllegalStateException("release() requires holdAll() or holdFirst(N) to have been set at builder time");
            holdLatch.countDown();
        }

        @Override
        public void close()
        {
            // Release any held messages so filter threads don't leak if the test fails early.
            if (holdLatch != null)
                holdLatch.countDown();
            filter.off();
        }
    }
}
