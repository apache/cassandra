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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.metrics.FailureDetectorMetrics;
import org.awaitility.core.ThrowingRunnable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests for
 * {@link org.apache.cassandra.metrics.GossipMetrics}
 * {@link org.apache.cassandra.metrics.FailureDetectorMetrics}
 */
public class GossipFDMetricsTest extends TestBaseImpl
{

    @Test
    public void testBasicMetrics() throws Exception
    {
        // setup a 3-node cluster
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .start())
        {
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            // verify that the metrics for nodes
            for (IInvokableInstance node : Arrays.asList(node1, node2, node3))
            {
                // Gossip
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> Gossiper.metrics.live.getValue())).isEqualTo(3L));
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> Gossiper.metrics.heartbeat.getValue())).isGreaterThan(0L));
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> Gossiper.metrics.unreachable.getValue())).isEqualTo(0L));
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> Gossiper.metrics.sendSynToUnreachable.getCount())).isGreaterThanOrEqualTo(0L));
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> Gossiper.metrics.sendSynToSeed.getCount())).isGreaterThanOrEqualTo(0L));
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> Gossiper.metrics.sendSynToCMS.getCount())).isGreaterThanOrEqualTo(0L));
                // FailureDetector
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> FailureDetectorMetrics.interpret.getCount())).isGreaterThan(0L));
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> FailureDetectorMetrics.report.getCount())).isGreaterThan(0L));
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> FailureDetectorMetrics.remove.getCount())).isGreaterThanOrEqualTo(0L));
                waitUntilAsserted(() -> assertThat(node.callOnInstance(() -> FailureDetectorMetrics.convict.getCount())).isGreaterThanOrEqualTo(0L));
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
}
