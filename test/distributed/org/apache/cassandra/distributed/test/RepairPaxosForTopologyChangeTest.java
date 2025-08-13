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

import java.util.List;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.service.StorageService;

import static java.util.Arrays.asList;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.action.GossipHelper.bootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.pullSchemaFrom;
import static org.apache.cassandra.distributed.action.GossipHelper.statusToBootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RepairPaxosForTopologyChangeTest extends TestBaseImpl
{
    @Test
    public void isBoostrapFailedOnRepairPaxosForTopologyChangeTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP).set("paxos_variant", "v2"))
                                        .withInstanceInitializer(BB::install)
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty("cassandra.join_ring", false, () -> newInstance.startup(cluster));
            cluster.forEach(statusToBootstrap(newInstance));

            long mark = cluster.get(2).logs().mark();
            try
            {
                cluster.run(asList(pullSchemaFrom(cluster.get(1)),
                                   bootstrap()),
                            newInstance.config().num());
                fail("Expect bootstrap failure");
            }
            catch (Exception e)
            {
                // expected
                assertTrue(e.getMessage().contains("Bootstrap did not complete successfully"));
                List<String> errors = cluster.get(2).logs().grepForErrors(mark).getResult();
                assertTrue(errors.toString(), errors.stream().anyMatch(s -> s.contains("Error while attempting to repair Paxos on topology change")));
            }
        }
    }

    @Test
    public void repairPaxosForTopologyChangeStrictMVOnlyTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP).set("paxos_variant", "v2").set("materialized_view_strict_consistency_enabled", true))
                                        .start())
        {
            // create 2 tables, one regular and one with strict MV
            cluster.schemaChange("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE test.test (id int PRIMARY KEY, v int)");
            cluster.schemaChange("CREATE TABLE test.test_mv (id int PRIMARY KEY, v int) WITH STRICT_MV_CONSISTENCY = true");

            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty("cassandra.join_ring", false, () -> newInstance.startup(cluster));
            cluster.forEach(statusToBootstrap(newInstance));
            long mark = cluster.get(2).logs().mark();
            cluster.get(2).runOnInstance(() -> DatabaseDescriptor.setSkipPaxosRepairOnTopologyChange(true));
            cluster.run(asList(pullSchemaFrom(cluster.get(1)),
                               bootstrap()),
                        newInstance.config().num());
            assertFalse(cluster.get(2).logs().grep(mark, "scheduling paxos cleanup for table test.test_mv").getResult().isEmpty());
        }
    }

    public static class BB
    {
        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num != 2)
                return;
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("repairPaxosForTopologyChange"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static void repairPaxosForTopologyChange(String reason)
        {
            throw new RuntimeException("Fail on repairPaxosForTopologyChange");
        }
    }
}
