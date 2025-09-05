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
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.apache.cassandra.repair.consistent.LocalSessionInfo.STATE;
import static org.apache.cassandra.repair.messages.RepairOption.INCREMENTAL_KEY;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class IncrementalRepairCleanupAfterNodeAddingTest extends TestBaseImpl
{
    @Test
    public void test() throws Exception
    {
        int originalNodeCount = 3;
        try (WithProperties withProperties = new WithProperties())
        {
            withProperties.setProperty("cassandra.repair_delete_timeout_seconds", "0");
            try (Cluster cluster = builder().withNodes(originalNodeCount)
                                            .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(originalNodeCount + 1, 1))
                                            .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                            .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                            .start())
            {
                populate(cluster, 0, 100);

                repair(cluster, KEYSPACE, ImmutableMap.of(INCREMENTAL_KEY, "true"));

                Thread.sleep(1); // to ensure that we crossed LocalSessions.AUTO_DELETE_TIMEOUT

                // to check that the session is still here (it is not superseded yet)
                cluster.get(1).runOnInstance(rethrow(() -> {
                    ActiveRepairService.instance.consistent.local.cleanup();
                    List<Map<String, String>> sessions = ActiveRepairService.instance.getSessions(true, null);
                    Assert.assertThat(sessions, hasSize(1));
                }));

                addNode(cluster);

                repair(cluster, KEYSPACE, ImmutableMap.of(INCREMENTAL_KEY, "true"));

                Thread.sleep(1); // to ensure that we crossed LocalSessions.AUTO_DELETE_TIMEOUT

                cluster.get(1).runOnInstance(rethrow(() -> {
                    ActiveRepairService.instance.consistent.local.cleanup();
                    List<Map<String, String>> sessions = ActiveRepairService.instance.getSessions(true, null);
                    Assert.assertThat(sessions, hasSize(1));
                }));
            }
        }
    }

    protected void addNode(Cluster cluster)
    {
        IInstanceConfig config = cluster.newInstanceConfig();
        config.set("auto_bootstrap", true);
        IInvokableInstance newInstance = cluster.bootstrap(config);
        newInstance.startup(cluster);
    }

    public static void populate(ICluster cluster, int from, int to)
    {
        populate(cluster, from, to, 1, 3, ConsistencyLevel.QUORUM);
    }

    public static void populate(ICluster cluster, int from, int to, int coord, int rf, ConsistencyLevel cl)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "};"));
        cluster.schemaChange(withKeyspace("CREATE TABLE IF NOT EXISTS %s.repair_add_node_test (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).execute(withKeyspace("INSERT INTO %s.repair_add_node_test (pk, ck, v) VALUES (?, ?, ?)"),
                                               cl, i, i, i);
        }
    }

    static void repair(ICluster<IInvokableInstance> cluster, String keyspace, Map<String, String> options)
    {
        cluster.get(1).runOnInstance(rethrow(() -> {
            SimpleCondition await = new SimpleCondition();
            StorageService.instance.repair(keyspace, options, ImmutableList.of((tag, event) -> {
                if (event.getType() == ProgressEventType.COMPLETE)
                    await.signalAll();
            })).right.get();
            await.await(1L, MINUTES);

            // local sessions finalization happens asynchronously
            // so to avoid race condition and flakiness for the test we wait explicitly for local sessions to finalize
            await().pollInterval(10, MILLISECONDS)
                   .atMost(60, SECONDS)
                   .until(() -> {
                       List<Map<String, String>> sessions = ActiveRepairService.instance.getSessions(true, null);
                       for (Map<String, String> sessionInfo : sessions)
                           if (!sessionInfo.get(STATE).equals(ConsistentSession.State.FINALIZED.toString()))
                               return false;
                       return true;
                   });
        }));
    }
}
