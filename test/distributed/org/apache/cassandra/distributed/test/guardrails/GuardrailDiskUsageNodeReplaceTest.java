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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.disk.usage.DiskUsageBroadcaster;
import org.apache.cassandra.service.disk.usage.DiskUsageState;

public class GuardrailDiskUsageNodeReplaceTest extends TestBaseImpl
{
    private Cluster cluster;

    @Before
    public void setupCluster() throws IOException
    {
        CassandraRelevantProperties.DISK_USAGE_MONITOR_INTERVAL_MS.setInt(100);
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        cluster = init(Cluster.build(2)
                              .withInstanceInitializer(GuardrailDiskUsageTest.DiskStateInjection::install)
                              .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK, Feature.NATIVE_PROTOCOL)
                                                .set("data_disk_usage_max_disk_size", "10GiB")
                                                .set("data_disk_usage_percentage_warn_threshold", 98)
                                                .set("data_disk_usage_percentage_fail_threshold", 98)
                                                .set("authenticator", "PasswordAuthenticator")
                                                .set("initial_location_provider", "SimpleLocationProvider")
                              )
                              .withTokenSupplier(node -> even.token(node == 3 ? 2 : node))
                              .start()
        );
    }

    @After
    public void tearDown() throws IOException
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testDiskUsageBroadcasterWhenNodeReplacedShouldRemoveUsageInfo() throws IOException
    {
        GuardrailDiskUsageTest.DiskStateInjection.setState(cluster, 2, DiskUsageState.FULL);
        IInvokableInstance nodeToRemove = cluster.get(2);
        ClusterUtils.stopUnchecked(nodeToRemove);
        IInvokableInstance replacingNode = ClusterUtils.replaceHostAndStart(cluster, nodeToRemove, props -> {
            props.set(CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
        });
        ClusterUtils.awaitRingJoin(cluster.get(1), replacingNode);
        ClusterUtils.awaitRingJoin(replacingNode, cluster.get(1));
        boolean hasStuffedOrFullNode = cluster.get(1).callOnInstance(() -> DiskUsageBroadcaster.instance.hasStuffedOrFullNode());
        Assert.assertFalse(hasStuffedOrFullNode);
    }
}
