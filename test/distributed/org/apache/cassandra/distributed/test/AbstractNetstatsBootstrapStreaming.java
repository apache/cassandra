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

import java.util.concurrent.Future;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public abstract class AbstractNetstatsBootstrapStreaming extends AbstractNetstatsStreaming
{
    protected void executeTest(final boolean streamEntireSSTables,
                               final boolean compressionEnabled) throws Exception
    {
        final Cluster.Builder builder = builder().withNodes(1)
                                                 .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                                                 .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                                 .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                             .set("stream_throughput_outbound_megabits_per_sec", 1)
                                                                             .set("compaction_throughput_mb_per_sec", 1)
                                                                             .set("stream_entire_sstables", streamEntireSSTables));

        try (final Cluster cluster = builder.withNodes(1).start())
        {
            // populate data only against 1 node first

            createTable(cluster, 1, compressionEnabled);

            cluster.get(1).nodetoolResult("disableautocompaction", "netstats_test").asserts().success();

            if (compressionEnabled)
            {
                populateData(true);
            }
            else
            {
                populateData(false);
            }

            cluster.get(1).flush("netstats_test");

            // then bootstrap the second one, upon joining,
            // we should see that netstats shows how SSTables are being streamed on the first node

            final IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);

            IInvokableInstance secondNode = cluster.bootstrap(config);

            final Future<?> startupRunnable = executorService.submit((Runnable) secondNode::startup);
            final Future<AbstractNetstatsStreaming.NetstatResults> netstatsFuture = executorService.submit(new NetstatsCallable(cluster.get(1)));

            final AbstractNetstatsStreaming.NetstatResults results = netstatsFuture.get(1, MINUTES);
            startupRunnable.get(2, MINUTES);

            results.assertSuccessful();

            AbstractNetstatsStreaming.NetstatsOutputParser.validate(AbstractNetstatsStreaming.NetstatsOutputParser.parse(results));
        }
    }
}
