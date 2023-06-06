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
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.ToolRunner;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingIs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getDirectories;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.shared.ClusterUtils.updateAddress;

public class IPMembershipTest extends TestBaseImpl
{
    /**
     * Port of replace_address_test.py::fail_without_replace_test to jvm-dtest
     */
    @Test
    public void sameIPFailWithoutReplace() throws IOException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .start())
        {
            IInvokableInstance nodeToReplace = cluster.get(3);

            ToolRunner.invokeCassandraStress("write", "n=10000", "-schema", "replication(factor=3)", "-port", "native=9042").assertOnExitCode();

            for (boolean auto_bootstrap : Arrays.asList(true, false))
            {
                stopUnchecked(nodeToReplace);
                getDirectories(nodeToReplace).forEach(FileUtils::deleteRecursive);

                nodeToReplace.config().set("auto_bootstrap", auto_bootstrap);

                // we need to override the host id because otherwise the node will not be considered as a new node
                ((InstanceConfig) nodeToReplace.config()).setHostId(UUID.randomUUID());

                Assertions.assertThatThrownBy(() -> nodeToReplace.startup())
                          .hasMessage("A node with address /127.0.0.3:7012 already exists, cancelling join. Use " +
                                      REPLACE_ADDRESS.getKey() + " if you want to replace this node.");
            }
        }
    }

    /**
     * Tests the behavior if a node restarts with a different IP.
     */
    @Test
    public void startupNewIP() throws IOException, InterruptedException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                        // disable DistributedTestSnitch as it tries to query before we setup
                                                        .set("endpoint_snitch", "org.apache.cassandra.locator.SimpleSnitch"))
                                      .start())
        {
            IInvokableInstance nodeToReplace = cluster.get(3);

            ToolRunner.invokeCassandraStress("write", "n=10000", "-schema", "replication(factor=3)", "-port", "native=9042").assertOnExitCode();

            stopUnchecked(nodeToReplace);

            // change the IP of the node
            updateAddress(nodeToReplace, "127.0.0.4");

            nodeToReplace.startup();

            // gossip takes some time, wait for the other nodes to see this one updated
            ClusterUtils.awaitRingJoin(cluster.get(1), "127.0.0.4");
            ClusterUtils.awaitRingJoin(cluster.get(2), "127.0.0.4");

            Set<String> expected = ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.4");
            cluster.forEach(i -> assertRingIs(i, expected));

            ToolRunner.invokeCassandraStress("read", "n=10000", "no-warmup", "-port", "native=9042").assertOnExitCode();
        }
    }
}
