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

import java.io.IOException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class ForceRepairTest extends TestBaseImpl
{
    private Cluster cluster;

    private static Cluster create(Consumer<IInstanceConfig> configModifier, int clusterSize, int replicationFactor) throws IOException
    {
        configModifier = configModifier.andThen(
        config -> config.set("hinted_handoff_enabled", false)
                        .set("commitlog_sync_batch_window_in_ms", 5)
                        .with(NETWORK)
                        .with(GOSSIP)
        );

        return init(Cluster.build().withNodes(clusterSize).withConfig(configModifier).start(), replicationFactor);
    }

    void forceRepair(ICluster<IInvokableInstance> cluster, Integer... downClusterIds) throws Exception
    {
        RepairTest.populate(cluster, "{'enabled': false}");
        Stream.of(downClusterIds).forEach(id -> cluster.get(id).shutdown());
        RepairTest.repair(cluster, ImmutableMap.of("forceRepair", "true"));
    }

    @After
    public void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testForcedNormalRepairWithOneNodeDown() throws Exception
    {
        cluster = create(config -> {}, 4, 3);
        forceRepair(cluster, 3); // shutdown node 3 after inserting
        DistributedRepairUtils.assertParentRepairSuccess(cluster, 1, KEYSPACE, "test", row -> {
            Set<String> successfulRanges = row.getSet("successful_ranges");
            Set<String> requestedRanges = row.getSet("requested_ranges");
            Assert.assertNotNull("Found no successful ranges", successfulRanges);
            Assert.assertNotNull("Found no requested ranges", requestedRanges);
            Assert.assertEquals("Requested ranges count should equals to replication factor", 3, requestedRanges.size());
            Assert.assertTrue("Given clusterSize = 4, RF = 3 and 1 node down in the replica set, it should yield only 1 successful repaired range.",
                              successfulRanges.size() == 1 && !successfulRanges.contains("")); // the successful ranges set should not only contain empty string
        });
    }
}
