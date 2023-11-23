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
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.cassandra.distributed.test.TransientRangeMovementTest.OPPTokens;
import static org.apache.cassandra.distributed.test.TransientRangeMovementTest.assertAllContained;
import static org.apache.cassandra.distributed.test.TransientRangeMovementTest.localStrs;
import static org.apache.cassandra.distributed.test.TransientRangeMovementTest.populate;

@SuppressWarnings("unchecked")
public class TransientRangeMovement2Test extends TestBaseImpl
{
    @Test
    public void testMoveBackward() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(4)
                                           .withTokenSupplier(new OPPTokens())
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                           .withConfig(conf -> conf.set("transient_replication_enabled","true")
                                                                   .set("partitioner", "OrderPreservingPartitioner") // just makes it easier to read the tokens in the log
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            populate(cluster);
            cluster.get(3).nodetoolResult("move", "25").asserts().success();
            cluster.forEach(i -> i.nodetoolResult("cleanup").asserts().success());

            assertAllContained(localStrs(cluster.get(1)),
                               newArrayList("22", "24"),
                               Pair.create("00", "10"),
                               Pair.create("26", "50"));
            assertAllContained(localStrs(cluster.get(2)),
                               newArrayList("26", "28", "30", "32", "34", "36", "38", "40"),
                               Pair.create("00", "20"),
                               Pair.create("41", "50"));
            assertAllContained(localStrs(cluster.get(3)),
                               newArrayList("42", "44", "46", "48", "00", "02", "04", "06", "08", "10"),
                               Pair.create("11", "25"));
            assertAllContained(localStrs(cluster.get(4)),
                               newArrayList("12", "14", "16", "18", "20"),
                               Pair.create("21", "40"));
        }
    }

    @Test
    public void testMoveForward() throws IOException, ExecutionException, InterruptedException
    {

        try (Cluster cluster = init(Cluster.build(4)
                                           .withTokenSupplier(new OPPTokens())
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                           .withConfig(conf -> conf.set("transient_replication_enabled","true")
                                                                   .set("partitioner", "OrderPreservingPartitioner") // just makes it easier to read the tokens in the log
                                                                   .set("hinted_handoff_enabled", "false")
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            populate(cluster);
            cluster.get(1).nodetoolResult("move", "15").asserts().success();
            cluster.forEach((i) -> i.nodetoolResult("cleanup").asserts().success());
            assertAllContained(localStrs(cluster.get(1)),
                               newArrayList("22", "24", "26", "28", "30"),
                               Pair.create("00", "15"),
                               Pair.create("31", "50"));
            assertAllContained(localStrs(cluster.get(2)),
                               newArrayList("32", "34", "36", "38", "40"),
                               Pair.create("00", "20"),
                               Pair.create("41", "50"));
            assertAllContained(localStrs(cluster.get(3)),
                               newArrayList("42", "44", "46", "48", "00", "02", "04", "06", "08", "10", "12", "14"),
                               Pair.create("16", "30"));
            assertAllContained(localStrs(cluster.get(4)),
                               newArrayList("16", "18", "20"),
                               Pair.create("21", "40"));
        }
    }



    @Test
    public void testRebuild() throws ExecutionException, InterruptedException, IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withTokenSupplier(new OPPTokens())
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                           .withConfig(conf -> conf.set("transient_replication_enabled","true")
                                                                   .set("partitioner", "OrderPreservingPartitioner")
                                                                   .set("hinted_handoff_enabled", "false")
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            populate(cluster);
            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", false);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup();
            cluster.forEach(i -> i.nodetoolResult("cleanup").asserts().success());
            cluster.get(4).nodetoolResult("rebuild", "-ks", "tr", "--tokens", "(15, 18],(20,25]").asserts().success();
            assertAllContained(localStrs(cluster.get(4)),
                               newArrayList(),
                               Pair.create("16", "18"),
                               Pair.create("21", "25"));
        }
    }
}
