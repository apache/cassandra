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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairUtilsV2;
import org.apache.cassandra.repair.AutoRepairV2;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.state.AutoRepairStateFactory;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamSession;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.schema.SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AutoRepairBootstrapRepairRunnableTest extends TestBaseImpl
{
    @Test
    public void repairRunnableTest() throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.setBoolean(true);

        int originalNodeCount = 5;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withDynamicPortAllocation(false)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(BBStreamFailure::install)
                                        .start())
        {
            InetSocketAddress node2Address = cluster.get(2).broadcastAddress();
            InetSocketAddress node3Address = cluster.get(3).broadcastAddress();
            InetSocketAddress node4Address = cluster.get(4).broadcastAddress();

            Set<Range<Token>> tokenRangesNode2 = getTokens(cluster, 2);
            populate(cluster, 0, 100, 1, 3, ConsistencyLevel.QUORUM);

            // Make node 1 stream fail
            cluster.get(1).runOnInstance(
            () -> {
                // verify that the normal node (cluster.get(1)) returns "NOT_MY_TURN" when probed for "bootstrap" repair type
                assertEquals(AutoRepairUtilsV2.RepairTurn.NOT_MY_TURN, AutoRepairStateFactory.getAutoRepairState(AutoRepairConfig.RepairType.bootstrap).calcRepairTurn(null));
                BBStreamFailure.failStream.set(true);
            }
            );

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            config
            .set("auto_repair",
                 ImmutableMap.of(
                 "repair_type_overrides",
                 ImmutableMap.of(AutoRepairConfig.RepairType.bootstrap.toString(),
                                 ImmutableMap.of(
                                 "initial_scheduler_delay_in_sec", "5",
                                 "enabled", "true",
                                 "parallel_repair_count_in_group", "1",
                                 "parallel_repair_percentage_in_group", "0",
                                 "min_repair_interval_in_hours", "-1"))))
            .set("auto_repair.enabled", "true")
            .set("auto_repair.repair_check_interval_in_sec", "10")
            .set("auto_repair.repair_task_min_duration", "0s");


            cluster.get(2).shutdown();
            System.setProperty("cassandra.replace_address", node2Address.getHostName());
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup(cluster);
            newInstance.logs().watchFor("Stream failed");

            // Make node 1 stream normal
            cluster.get(1).runOnInstance(
            () -> {
                BBStreamFailure.failStream.set(false);
            }
            );

            // verify that calling RepairRunnable::getNeighborsAndRanges() for UN node (node1) does not find anything
            // when asked for node2's token ranges because by default the RepairRunnable::getNeighborsAndRanges on node1
            // looks among its local ranges only.
            cluster.get(1).runOnInstance(
            () -> {
                AutoRepairV2.instance.setup();

                assertEquals(AutoRepairUtilsV2.RepairTurn.NOT_MY_TURN, AutoRepairStateFactory.getAutoRepairState(AutoRepairConfig.RepairType.bootstrap).calcRepairTurn(null));
                assertFalse(AutoRepairUtilsV2.isBootstrapRepair());
                RepairOption option = new RepairOption(RepairParallelism.PARALLEL, true, true, false,
                                                       AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(AutoRepairConfig.RepairType.full), tokenRangesNode2,
                                                       !tokenRangesNode2.isEmpty(), false, false, PreviewKind.NONE, false, true, false, false);
                RepairRunnable repairRunnable = new RepairRunnable(StorageService.instance, 0, option, KEYSPACE);
                try
                {
                    repairRunnable.getNeighborsAndRanges();
                    fail("Should have thrown an exception");
                }
                catch (RuntimeException | RepairException e)
                {
                    assertTrue(e.toString(), e.toString().contains("Nothing to repair for"));
                }
            }
            );

            // verify that the bootstrapping node (UJ) "newInstance" returns "MY_TURN" when probed for "bootstrap" repair type
            // RepairRunnable::getNeighborsAndRanges on the bootstrapping node should return the neighbors for node2's token ranges
            // (127.0.0.2, 127.0.0.4, 127.0.0.4)
            newInstance.runOnInstance(
            () -> {
                AutoRepairV2.instance.setup();

                AutoRepairConfig autoRepairConfig = AutoRepairService.instance.getAutoRepairConfig();
                assertEquals(AutoRepairUtilsV2.RepairTurn.MY_TURN, AutoRepairStateFactory.getAutoRepairState(AutoRepairConfig.RepairType.bootstrap).calcRepairTurn(null));
                assertTrue(AutoRepairUtilsV2.isBootstrapRepair());
                RepairOption option = new RepairOption(RepairParallelism.PARALLEL, true, true, false,
                                                       autoRepairConfig.getRepairThreads(AutoRepairConfig.RepairType.full), tokenRangesNode2,
                                                       !tokenRangesNode2.isEmpty(), false, false, PreviewKind.NONE, false, true, false, false);
                RepairRunnable repairRunnable = new RepairRunnable(StorageService.instance, 0, option, KEYSPACE);
                RepairRunnable.NeighborsAndRanges neighborsAndRanges;
                try
                {
                    neighborsAndRanges = repairRunnable.getNeighborsAndRanges();
                }
                catch (RepairException e)
                {
                    throw new RuntimeException(e);
                }
                assertEquals(new TreeSet<>(Arrays.asList(node2Address.getHostName(), node3Address.getHostName(), node4Address.getHostName())),
                             neighborsAndRanges.participants.stream().map(p -> p.getHostAddress(false))
                                                            .collect(Collectors.toCollection(TreeSet::new)));

                // disable bootstrap repair type and it should throw an exception
                // because the local token range is empty
                autoRepairConfig.setAutoRepairEnabled(AutoRepairConfig.RepairType.bootstrap, false);
                try
                {
                    repairRunnable.getNeighborsAndRanges();
                    fail("Should have thrown an exception");
                }
                catch (RuntimeException | RepairException e)
                {
                    assertTrue(e.toString(), e.toString().contains("Nothing to repair for"));
                }
            }
            );
        }
    }

    public static void populate(ICluster cluster, int from, int to, int coord, int rf, ConsistencyLevel cl)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "};");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                               cl, i, i, i);
        }
    }

    private static Set<Range<Token>> getTokens(Cluster cluster, int nodeId)
    {
        return cluster.get(nodeId).callOnInstance(() -> {
            try
            {
                AutoRepairV2.instance.setup();

                Set<Range<Token>> ranges = new HashSet<>();
                Collection<Range<Token>> tokenRanges = StorageService.instance.getPrimaryRanges(AUTO_REPAIR_KEYSPACE_NAME);
                for (Range<Token> token : tokenRanges)
                {
                    ranges.add(token);
                }
                return ranges;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    public static class BBStreamFailure
    {
        public static final AtomicBoolean failStream = new AtomicBoolean();

        public static void install(ClassLoader cl, Integer i)
        {
            new ByteBuddy().rebase(StreamSession.class)
                           .method(named("startStreamingFiles"))
                           .intercept(MethodDelegation.to(BBStreamFailure.class))
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
