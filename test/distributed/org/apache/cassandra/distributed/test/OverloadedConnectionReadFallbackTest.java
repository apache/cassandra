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

package org.apache.cassandra.distributed.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundConnections;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

public class OverloadedConnectionReadFallbackTest extends TestBaseImpl
{
    private static final String TABLE = "t";
    private static final int SEED_ROWS = 200;

    private static final int COORDINATOR = 1;
    private static final String COORDINATOR_ADDRESS = "127.0.0.1";

    private static final String SMALL_SEND_QUEUE = "4KiB";

    private static final int READ_THREADS = 128;

    private static final int READ_SECONDS = 3;

    private static final String UNAVAILABLE = "Cannot achieve consistency level";

    private static final String TIMED_OUT = "Operation timed out - received only";

    @Test
    public void oneOverloadedConnectionFallsBackToTheSpareReplica() throws Throwable
    {
        assertOverloadedConnectionsDoNotFailReads(3, new int[]{ 3 });
    }

    @Test
    public void twoOverloadedConnectionsFallBackTwice() throws Throwable
    {
        assertOverloadedConnectionsDoNotFailReads(5, new int[]{ 3, 4 });
    }

    private void assertOverloadedConnectionsDoNotFailReads(int rf, int[] downNodes) throws Throwable
    {
        int quorum = rf / 2 + 1;
        List<String> downAddresses = addresses(downNodes);

        try (Cluster cluster = startCluster(rf + 1, rf))
        {
            seed(cluster);

            List<Integer> keys = keysContactingAllOf(cluster, quorum, downAddresses);
            assertThat(keys)
                .as("expected keys whose replicas exclude the coordinator and contact " + downAddresses)
                .isNotEmpty();

            for (int node : downNodes)
                ClusterUtils.stopAbrupt(cluster, cluster.get(node));

            ReadOutcome outcome = hammerReads(cluster, keys, ConsistencyLevel.QUORUM);

            for (String downAddress : downAddresses)
                assertThat(overloadedMessages(cluster, downAddress))
                    .as("the send queue for " + downAddress + " should overload")
                    .isGreaterThan(0L);

            assertThat(overloadSpeculativeRetries(cluster))
                .as("a dropped read message should move the read to another replica")
                .isGreaterThan(0L);

            assertThat(outcome.firstFailure)
                .as("a QUORUM read with enough live replicas should not fail because a connection is overloaded"
                    + " (" + outcome.timeouts + " reads timed out on the expiry route)")
                .isNull();

            assertThat(outcome.successes)
                .as("reads should continue to be served from the remaining replicas")
                .isGreaterThan(0);

            assertThat(outcome.wrongResults)
                .as("the fallback replicas should return the seeded row")
                .isEqualTo(0);
        }
    }

    private Cluster startCluster(int nodes, int rf) throws Throwable
    {
        return init(builder().withNodes(nodes)
                             .withConfig(config -> {
                                 config.with(NETWORK, GOSSIP);
                                 config.set("read_request_timeout", "5s");
                                 config.set("write_request_timeout", "5s");
                                 config.set("internode_application_send_queue_capacity", SMALL_SEND_QUEUE);
                                 config.set("dynamic_snitch", false);
                                 config.set("read_fallback_on_overloaded_connection", true);
                             })
                             .start(), rf);
    }

    private void seed(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v int)"));
        for (int pk = 0; pk < SEED_ROWS; pk++)
            cluster.coordinator(COORDINATOR)
                   .execute(withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (" + pk + ", 1)"),
                            ConsistencyLevel.ALL);
    }

    private List<Integer> keysContactingAllOf(Cluster cluster, int quorum, List<String> required)
    {
        String keyspace = KEYSPACE;
        String table = TABLE;
        int keyCount = SEED_ROWS;
        String coordinator = COORDINATOR_ADDRESS;
        ArrayList<String> mustContact = new ArrayList<>(required);

        return cluster.get(COORDINATOR).callOnInstance(() -> {
            Keyspace ks = Keyspace.open(keyspace);
            List<Integer> selected = new ArrayList<>();

            for (int pk = 0; pk < keyCount; pk++)
            {
                ByteBuffer key = Int32Type.instance.decompose(pk);
                Token token = ks.getColumnFamilyStore(table).metadata().partitioner.decorateKey(key).getToken();
                EndpointsForToken sorted = ReplicaLayout.forTokenReadLiveSorted(ks.getReplicationStrategy(), token)
                                                        .natural();

                List<String> ordered = new ArrayList<>();
                for (Replica replica : sorted)
                    ordered.add(replica.endpoint().getHostAddress(false));

                if (ordered.contains(coordinator))
                    continue;
                if (ordered.subList(0, Math.min(quorum, ordered.size())).containsAll(mustContact))
                    selected.add(pk);
            }
            return selected;
        });
    }

    private long overloadSpeculativeRetries(Cluster cluster)
    {
        String keyspace = KEYSPACE;
        String table = TABLE;

        return cluster.get(COORDINATOR).callOnInstance(
            () -> Keyspace.open(keyspace).getColumnFamilyStore(table).metric.overloadSpeculativeRetries.getCount());
    }

    private long overloadedMessages(Cluster cluster, String peerAddress)
    {
        return cluster.get(COORDINATOR).callOnInstance(() -> {
            long overloaded = 0;
            for (Map.Entry<InetAddressAndPort, OutboundConnections> entry :
                 MessagingService.instance().channelManagers.entrySet())
            {
                if (!entry.getKey().getHostAddress(false).equals(peerAddress))
                    continue;

                OutboundConnections connections = entry.getValue();
                overloaded += connections.small.overloadedCount()
                              + connections.large.overloadedCount()
                              + connections.urgent.overloadedCount();
            }
            return overloaded;
        });
    }

    private ReadOutcome hammerReads(Cluster cluster, List<Integer> keys, ConsistencyLevel cl)
    throws InterruptedException
    {
        String query = withKeyspace("SELECT pk, v FROM %s." + TABLE + " WHERE pk = ?");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(READ_SECONDS);

        AtomicReference<String> firstFailure = new AtomicReference<>();
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger timeouts = new AtomicInteger();
        AtomicInteger wrongResults = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(READ_THREADS);
        try
        {
            for (int i = 0; i < READ_THREADS; i++)
            {
                final int offset = i;
                pool.submit(() -> {
                    int index = offset;
                    while (System.nanoTime() < deadline && firstFailure.get() == null)
                    {
                        int pk = keys.get(Math.floorMod(index, keys.size()));
                        try
                        {
                            Object[][] rows = cluster.coordinator(COORDINATOR).execute(query, cl, pk);
                            if (rows.length != 1 || !Integer.valueOf(pk).equals(rows[0][0])
                                || !Integer.valueOf(1).equals(rows[0][1]))
                                wrongResults.incrementAndGet();
                            else
                                successes.incrementAndGet();
                        }
                        catch (Throwable t)
                        {
                            String rendered = t.toString();

                            if (rendered.contains(UNAVAILABLE))
                                return;

                            // A message that reached the send queue before it filled cannot be detected
                            // at send time, so it expires and the read times out. The fallback cannot
                            // prevent that, so a timeout is not a failure of the route under test.
                            if (rendered.contains(TIMED_OUT))
                                timeouts.incrementAndGet();
                            else
                                firstFailure.compareAndSet(null, rendered);
                        }
                        index += READ_THREADS;
                    }
                });
            }
            pool.shutdown();
            pool.awaitTermination(READ_SECONDS * 3L, TimeUnit.SECONDS);
        }
        finally
        {
            pool.shutdownNow();
        }

        return new ReadOutcome(firstFailure.get(), successes.get(), timeouts.get(), wrongResults.get());
    }

    private static List<String> addresses(int[] nodes)
    {
        List<String> addresses = new ArrayList<>(nodes.length);
        for (int node : nodes)
            addresses.add("127.0.0." + node);
        return addresses;
    }

    private static final class ReadOutcome
    {
        final String firstFailure;
        final int successes;
        final int timeouts;
        final int wrongResults;

        ReadOutcome(String firstFailure, int successes, int timeouts, int wrongResults)
        {
            this.firstFailure = firstFailure;
            this.successes = successes;
            this.timeouts = timeouts;
            this.wrongResults = wrongResults;
        }
    }
}
