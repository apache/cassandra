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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;

import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class AutoRepairBootstrapRepairE2ETestHelper extends TestBaseImpl
{
    public static void helperBootstrapTest(Cluster.Builder builder, boolean bootstrapRepairEnabled) throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.setBoolean(true);

        int originalNodeCount = 3;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder.withNodes(originalNodeCount)
                                        .withDynamicPortAllocation(false)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100, 1, 3, ConsistencyLevel.QUORUM);

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            config
            .set("auto_repair",
                 ImmutableMap.of(
                 "repair_type_overrides",
                 ImmutableMap.of(AutoRepairConfig.RepairType.BOOTSTRAP.getConfigName(),
                                 ImmutableMap.<String, String>builder()
                                             .put("initial_scheduler_delay", "5s")
                                             .put("enabled", Boolean.toString(bootstrapRepairEnabled))
                                             .put("parallel_repair_count", "1")
                                             .put("parallel_repair_percentage", "0")
                                             .put("min_repair_interval", "0s")
                                             .put("repair_max_retries", "0")
                                             .build()
                 )))
            .set("auto_repair.enabled", "true")
            .set("auto_repair.repair_check_interval", "10s")
            .set("auto_repair.repair_task_min_duration", "0s");

            InetSocketAddress node2Address = cluster.get(2).broadcastAddress();

            IInvokableInstance newInstance = cluster.bootstrap(config);
            cluster.get(2).shutdown();
            System.setProperty("cassandra.replace_address", node2Address.getHostName());
            newInstance.startup(cluster);
            if (bootstrapRepairEnabled)
            {
                newInstance.logs().watchFor("Bootstrap Repair has completed!");
                newInstance.logs().watchFor("Bootstrap repair during node replacement succeeded");
            }
            else
            {
                newInstance.logs().watchFor("Bootstrap repair either not enabled or failed");
            }
            newInstance.runOnInstance(
            () -> {
                assertEquals(1, AutoRepairMetricsManager.getMetrics(AutoRepairConfig.RepairType.BOOTSTRAP).bootstrapRepairStarted.getCount());
                if (bootstrapRepairEnabled)
                {
                    assertEquals(1, AutoRepairMetricsManager.getMetrics(AutoRepairConfig.RepairType.BOOTSTRAP).bootstrapRepairSucceded.getCount());
                }
                else
                {
                    assertEquals(1, AutoRepairMetricsManager.getMetrics(AutoRepairConfig.RepairType.BOOTSTRAP).bootstrapRepairDisabledOrFailed.getCount());
                }
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
}
