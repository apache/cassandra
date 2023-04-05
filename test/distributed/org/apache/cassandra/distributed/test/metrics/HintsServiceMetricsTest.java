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

package org.apache.cassandra.distributed.test.metrics;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.Metrics;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.concurrent.Future;
import org.awaitility.core.ThrowingRunnable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests {@link HintsServiceMetrics}.
 */
public class HintsServiceMetricsTest extends TestBaseImpl
{
    private static final int NUM_ROWS = 100;
    private static final int NUM_FAILURES_PER_NODE = 5;
    private static final int NUM_TIMEOUTS_PER_NODE = 3;

    @Test
    public void testHintsServiceMetrics() throws Exception
    {
        // setup a 3-node cluster with a bytebuddy injection that makes the writting of some hints to fail
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .withInstanceInitializer(FailHints::install)
                                        .start())
        {
            cluster.setUncaughtExceptionsFilter(t -> "Injected failure".equals(t.getMessage()));
            // setup a message filter to drop some of the hint request messages from node1
            AtomicInteger hintsNode2 = new AtomicInteger();
            AtomicInteger hintsNode3 = new AtomicInteger();
            cluster.filters()
                   .verbs(Verb.HINT_REQ.id)
                   .from(1)
                   .messagesMatching((from, to, message) ->
                                     (to == 2 && hintsNode2.incrementAndGet() <= NUM_TIMEOUTS_PER_NODE) ||
                                     (to == 3 && hintsNode3.incrementAndGet() <= NUM_TIMEOUTS_PER_NODE))
                   .drop();

            // setup a message filter to drop mutations requests from node1, so it creates hints for those mutations
            AtomicBoolean dropWritesForNode2 = new AtomicBoolean(false);
            AtomicBoolean dropWritesForNode3 = new AtomicBoolean(false);
            cluster.filters()
                   .verbs(Verb.MUTATION_REQ.id)
                   .from(1)
                   .messagesMatching((from, to, message) ->
                                     (to == 2 && dropWritesForNode2.get()) ||
                                     (to == 3 && dropWritesForNode3.get()))
                   .drop();

            // fix under replicated keyspaces so they don't produce hint requests while we are dropping mutations
            fixDistributedSchemas(cluster);

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int)"));

            ICoordinator coordinator = cluster.coordinator(1);
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            // write the first half of the rows with the second node dropping mutation requests,
            // so some hints will be created for that node
            dropWritesForNode2.set(true);
            for (int i = 0; i < NUM_ROWS / 2; i++)
                coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), QUORUM, i, i);
            dropWritesForNode2.set(false);

            // write the second half of the rows with the third node dropping mutations requests,
            // so some hints will be created for that node
            dropWritesForNode3.set(true);
            for (int i = NUM_ROWS / 2; i < NUM_ROWS; i++)
                coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), QUORUM, i, i);
            dropWritesForNode3.set(false);

            // wait until all the hints have been successfully applied to the nodes that have been dropping mutations
            waitUntilAsserted(() -> assertThat(countRows(node2)).isEqualTo(countRows(node3)).isEqualTo(NUM_ROWS));

            // Verify the metrics for the coordinator node, which is the only one actually sending hints.
            // The hint delivery errors that we have injected should have made the service try to send them again.
            // These retries are done periodically and in pages, so the retries may send again some of the hints that
            // were already successfully sent. This way, there may be more succeeded hints than actual hints/rows.
            waitUntilAsserted(() -> assertThat(countHintsSucceeded(node1)).isGreaterThanOrEqualTo(NUM_ROWS));
            waitUntilAsserted(() -> assertThat(countHintsFailed(node1)).isEqualTo(NUM_FAILURES_PER_NODE * 2));
            waitUntilAsserted(() -> assertThat(countHintsTimedOut(node1)).isEqualTo(NUM_TIMEOUTS_PER_NODE * 2));

            // verify delay metrics
            long numGlobalDelays = countGlobalDelays(node1);
            assertThat(numGlobalDelays).isGreaterThanOrEqualTo(NUM_ROWS);
            assertThat(countEndpointDelays(node1, node1)).isEqualTo(0);
            assertThat(countEndpointDelays(node1, node2)).isGreaterThan(0).isLessThanOrEqualTo(numGlobalDelays);
            assertThat(countEndpointDelays(node1, node3)).isGreaterThan(0).isLessThanOrEqualTo(numGlobalDelays);
            assertThat(countEndpointDelays(node1, node2) + countEndpointDelays(node1, node3)).isGreaterThanOrEqualTo(numGlobalDelays);

            // verify that the metrics for the not-coordinator nodes are zero
            for (IInvokableInstance node : Arrays.asList(node2, node3))
            {
                assertThat(countHintsSucceeded(node)).isEqualTo(0);
                assertThat(countHintsFailed(node)).isEqualTo(0);
                assertThat(countHintsTimedOut(node)).isEqualTo(0);
                assertThat(countGlobalDelays(node)).isEqualTo(0);
                cluster.forEach(target -> assertThat(countEndpointDelays(node, target)).isEqualTo(0));
            }
        }
    }

    private static void waitUntilAsserted(ThrowingRunnable assertion)
    {
        await().atMost(5, MINUTES)
               .pollDelay(0, SECONDS)
               .pollInterval(1, SECONDS)
               .dontCatchUncaughtExceptions()
               .untilAsserted(assertion);
    }

    private static int countRows(IInvokableInstance node)
    {
        return node.executeInternal(withKeyspace("SELECT * FROM %s.t")).length;
    }

    @SuppressWarnings("Convert2MethodRef")
    private static Long countHintsSucceeded(IInvokableInstance node)
    {
        return node.callOnInstance(() -> HintsServiceMetrics.hintsSucceeded.getCount());
    }

    @SuppressWarnings("Convert2MethodRef")
    private static Long countHintsFailed(IInvokableInstance node)
    {
        return node.callOnInstance(() -> HintsServiceMetrics.hintsFailed.getCount());
    }

    @SuppressWarnings("Convert2MethodRef")
    private static Long countHintsTimedOut(IInvokableInstance node)
    {
        return node.callOnInstance(() -> HintsServiceMetrics.hintsTimedOut.getCount());
    }

    private static Long countGlobalDelays(IInvokableInstance node)
    {
        return getHistogramCount(node, "org.apache.cassandra.metrics.HintsService.Hint_delays");
    }

    private static Long countEndpointDelays(IInvokableInstance node, IInvokableInstance target)
    {
        return getHistogramCount(node, String.format("org.apache.cassandra.metrics.HintsService.Hint_delays-%s.%d",
                                                     target.broadcastAddress().getAddress(),
                                                     target.broadcastAddress().getPort()));
    }

    private static long getHistogramCount(IInvokableInstance node, String name)
    {
        return node.metrics()
                   .getHistograms(s -> s.equals(name), Metrics.MetricValue.COUNT)
                   .values()
                   .stream()
                   .findFirst()
                   .map(Math::round)
                   .orElse(0L);
    }

    /**
     * Bytebuddy injection to make the application of hints to fail on the destination node.
     */
    public static class FailHints
    {
        private static final AtomicInteger numHints = new AtomicInteger(0);

        private static void install(ClassLoader cl, int nodeNumber)
        {
            // we can ignore the coordinator node
            if (nodeNumber == 1)
                return;

            new ByteBuddy().rebase(Hint.class)
                           .method(named("applyFuture").and(takesArguments(0)))
                           .intercept(MethodDelegation.to(FailHints.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static Future<?> execute(@SuperCall Callable<Future<?>> r) throws Exception
        {
            if (numHints.incrementAndGet() <= NUM_FAILURES_PER_NODE)
                throw new RuntimeException("Injected failure");
            return r.call();
        }
    }
}
