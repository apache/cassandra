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

import com.google.common.collect.ImmutableMap;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairUtilsV2;
import org.apache.cassandra.repair.AutoRepairV2;
import org.apache.cassandra.repair.state.AutoRepairStateFactory;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.streaming.StreamSession;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.repair.AutoRepairConfig.RepairType.bootstrap;
import static org.apache.cassandra.repair.AutoRepairConfig.RepairType.full;
import static org.apache.cassandra.repair.AutoRepairConfig.RepairType.incremental;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoRepairInvokeBootstrapRepairOnReplacementTest extends TestBaseImpl
{
    @Test
    public void invokeBootstrapRepairE2E() throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.setBoolean(true);

        int originalNodeCount = 3;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withDynamicPortAllocation(false)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(AutoRepairInvokeBootstrapRepairOnReplacementTest.BBStreamFailure::install)
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
                 ImmutableMap.of(bootstrap.toString(),
                                 ImmutableMap.<String, String>builder()
                                             .put("initial_scheduler_delay_in_sec", "5")
                                             .put("enabled", "false")
                                             .put("parallel_repair_count_in_group", "1")
                                             .put("parallel_repair_percentage_in_group", "0")
                                             .put("min_repair_interval_in_hours", "-1")
                                             .put("repair_only_keyspaces", KEYSPACE).build()
                 )))
            .set("auto_repair.enabled", "true")
            .set("auto_repair.repair_check_interval_in_sec", "10")
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
                assertEquals(AutoRepairUtilsV2.RepairTurn.NOT_MY_TURN, AutoRepairStateFactory.getAutoRepairState(AutoRepairConfig.RepairType.bootstrap).calcRepairTurn(null));
                BBStreamFailure.failStream.set(false);
            }
            );
            // run bootstrap repair on the UJ node
            newInstance.runOnInstance(
            () -> {
                try
                {
                    AutoRepairService.setup();
                    AutoRepairConfig cfg = AutoRepairService.instance.getAutoRepairConfig();
                    cfg.setAutoRepairEnabled(bootstrap, true);
                    // we won't run a full repair on UJ-this is just to ensure that the config value for "full"
                    // and other repair types is preserved post-bootstrap repair round
                    cfg.setAutoRepairEnabled(full, true);

                    // "bootstrap" repair should be run successfully
                    validateConfigAndBootstrapRepairExecution(cfg, true);

                    AutoRepairV2.instance.getRepairState(bootstrap).setNodeRepairTimeInSec(0);

                    // "bootstrap" repair should not be run because it is disabled
                    validateConfigAndBootstrapRepairExecution(cfg, false);

                    // enable "bootstrap" repair but allocate an extremely short quota "1s"
                    // in this scenario; the expectation is that the bootstrap repair should abort after "1s" of execution
                    cfg.setAutoRepairEnabled(bootstrap, true);
                    AutoRepairUtilsV2.bootstrapRepairDurationUpperCap = new DurationSpec.LongSecondsBound("1s");

                    assertTrue(cfg.isAutoRepairEnabled(bootstrap));
                    assertTrue(cfg.isAutoRepairEnabled(full));
                    assertFalse(cfg.isAutoRepairEnabled(incremental));

                    boolean completedSuccessfully = AutoRepairUtilsV2.runBootstrapRepair();
                    assertFalse("One round of repair should not have completed as we want to abort earlier",
                                completedSuccessfully);
                    assertEquals(0,
                                 AutoRepairMetricsManager.getMetrics(bootstrap).nodeRepairTimeInSec.getValue().longValue());
                    assertEquals(1, AutoRepairMetricsManager.getMetrics(bootstrap).bootstrapRepairAborted.getCount());
                    // after one round, the "bootstrap" repair should be automatically disabled
                    // but the other repairs, such as "full" should continue to maintain its original enablement state
                    assertFalse(cfg.isAutoRepairEnabled(bootstrap));
                    assertTrue(cfg.isAutoRepairEnabled(full));
                    assertFalse(cfg.isAutoRepairEnabled(incremental));
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            );
        }
    }

    private static void validateConfigAndBootstrapRepairExecution(AutoRepairConfig cfg, boolean bootstrapRepairEnabled) throws InterruptedException
    {
        assertTrue("Bootstrap repair should not have been run previously",
                   AutoRepairMetricsManager.getMetrics(bootstrap).nodeRepairTimeInSec.getValue().longValue() == 0);
        if (bootstrapRepairEnabled)
        {
            assertTrue(cfg.isAutoRepairEnabled(bootstrap));
        }
        else
        {
            assertFalse(cfg.isAutoRepairEnabled(bootstrap));
        }
        assertTrue(cfg.isAutoRepairEnabled(full));
        assertFalse(cfg.isAutoRepairEnabled(incremental));

        boolean completedSuccessfully = AutoRepairUtilsV2.runBootstrapRepair();
        if (bootstrapRepairEnabled)
        {
            assertTrue("One round of repair should have completed successfully",
                       completedSuccessfully);
            assertTrue("Bootstrap repair could not finish one round",
                       AutoRepairMetricsManager.getMetrics(bootstrap).nodeRepairTimeInSec.getValue().longValue() > 0);
        }
        else
        {
            assertFalse("The bootstrap repair should not have been run as it was disabled",
                        completedSuccessfully);
            assertEquals(0,
                         AutoRepairMetricsManager.getMetrics(bootstrap).nodeRepairTimeInSec.getValue().longValue());
        }
        // after one round, the "bootstrap" repair should be automatically disabled
        // but the other repairs, such as "full" should continue to maintain its original enablement state
        assertFalse(cfg.isAutoRepairEnabled(bootstrap));
        assertTrue(cfg.isAutoRepairEnabled(full));
        assertFalse(cfg.isAutoRepairEnabled(incremental));
        assertEquals(0, AutoRepairMetricsManager.getMetrics(bootstrap).bootstrapRepairAborted.getCount());
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
                           .intercept(MethodDelegation.to(AutoRepairInvokeBootstrapRepairOnReplacementTest.BBStreamFailure.class))
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
