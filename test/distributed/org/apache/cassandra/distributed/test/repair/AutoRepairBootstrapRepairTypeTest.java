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

package org.apache.cassandra.distributed.test.repair;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.Util;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.repair.autorepair.AutoRepair;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.streaming.StreamSession;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType.BOOTSTRAP;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AutoRepairBootstrapRepairTypeTest extends TestBaseImpl
{
    @Test
    public void bootstrapAutoRepairTurn() throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.setBoolean(true);

        int originalNodeCount = 3;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withDynamicPortAllocation(false)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(AutoRepairBootstrapRepairTypeTest.BBStreamFailure::install)
                                        .start())
        {
            populate(cluster, 0, 100, 1, 3, ConsistencyLevel.QUORUM);

            // Make node 1 stream fail
            cluster.get(1).runOnInstance(
            () -> {
                BBStreamFailure.failStream.set(true);
            }
            );

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            config
            .set("auto_repair",
                 ImmutableMap.of(
                 "repair_type_overrides",
                 ImmutableMap.of(BOOTSTRAP.getConfigName(),
                                 ImmutableMap.<String, String>builder()
                                             .put("initial_scheduler_delay", "5s")
                                             .put("enabled", "false")
                                             .put("parallel_repair_count", "1")
                                             .put("parallel_repair_percentage", "0")
                                             .put("repair_max_retries", "0")
                                             .put("min_repair_interval", "0s").build()
                 )))
            .set("auto_repair.enabled", "true")
            .set("auto_repair.repair_check_interval", "2s")
            .set("auto_repair.repair_task_min_duration", "0s");

            InetSocketAddress node2Address = cluster.get(2).broadcastAddress();

            IInvokableInstance newInstance = cluster.bootstrap(config);
            cluster.get(2).shutdown();
            System.setProperty("cassandra.replace_address", node2Address.getHostName());
            newInstance.startup(cluster);
            newInstance.logs().watchFor("Stream failed");

            // Make node 1 stream normal
            cluster.get(1).runOnInstance(
            () -> {
                // verify that the normal node (cluster.get(1)) returns "NOT_MY_TURN" when probed for "bootstrap" repair type
                assertEquals(AutoRepairUtils.RepairTurn.NOT_MY_TURN, AutoRepairConfig.RepairType.getAutoRepairState(BOOTSTRAP).calcRepairTurn(null));
                BBStreamFailure.failStream.set(false);
            });
            // run bootstrap repair on the UJ node
            newInstance.runOnInstance(
            () -> {
                AutoRepairService.setup();
                AutoRepair.instance.setup();
                AutoRepairService.instance.getAutoRepairConfig().setAutoRepairEnabled(BOOTSTRAP, true);
                assertEquals(AutoRepairUtils.RepairTurn.MY_TURN, AutoRepairConfig.RepairType.getAutoRepairState(BOOTSTRAP).calcRepairTurn(null));
                assertTrue(AutoRepairUtils.isBootstrapRepair());

                // ensure that the "bootstrap" repair has finished one round
                AutoRepairMetrics bootstrapMetrics = AutoRepairMetricsManager.getMetrics(BOOTSTRAP);
                Util.spinAssert("AutoRepair has not yet completed one BOOTSTRAP repair cycle",
                                greaterThan(0L),
                                () -> bootstrapMetrics.nodeRepairTimeInSec.getValue().longValue(),
                                1,
                                TimeUnit.MINUTES);
            });
        }
    }

    public static void populate(ICluster cluster, int from, int to, int coord, int rf, ConsistencyLevel cl)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "};");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                               cl,
                                               i, i, i);
        }
    }

    public static class BBStreamFailure
    {
        public static final AtomicBoolean failStream = new AtomicBoolean();

        public static void install(ClassLoader cl, Integer i)
        {
            new ByteBuddy().rebase(StreamSession.class)
                           .method(named("startStreamingFiles"))
                           .intercept(MethodDelegation.to(AutoRepairBootstrapRepairTypeTest.BBStreamFailure.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }


        public static void startStreamingFiles(StreamSession.PrepareDirection prepareDirection, @SuperCall Callable<Boolean> zuper) throws Exception
        {
            if (failStream.get())
            {
                throw new RuntimeException("Trigger stream failure");
            }
            zuper.call();
        }
    }
}
