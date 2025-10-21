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

package org.apache.cassandra.distributed.test.log;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

public class KeyspaceParamsRecreateTest extends TestBaseImpl
{
    @Test
    public void bounceTest() throws Exception
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> config.set("default_keyspace_rf", "100"))
                                        .start())
        {
            cluster.coordinator(1).execute("CREATE KEYSPACE before_bounce WITH replication = {'class': 'SimpleStrategy'}", ConsistencyLevel.ALL);
            cluster.get(1).shutdown().get();
            cluster.get(1).config().set("default_keyspace_rf", "200");

            cluster.get(1).startup();
            cluster.coordinator(1).execute("CREATE KEYSPACE after_bounce WITH replication = {'class': 'SimpleStrategy'}", ConsistencyLevel.ALL);

            // The keyspace created before the bounce should preserve the initial default value. The one created after
            // updating should take the new value from config.
            cluster.stream().forEach(i -> {
                i.runOnInstance(() -> {
                    KeyspaceMetadata before_bounce = ClusterMetadata.current().schema.getKeyspace("before_bounce").getMetadata();
                    Assert.assertEquals("100", before_bounce.params.replication.options.get("replication_factor"));
                    KeyspaceMetadata after_bounce = ClusterMetadata.current().schema.getKeyspace("after_bounce").getMetadata();
                    Assert.assertEquals("200", after_bounce.params.replication.options.get("replication_factor"));
                });
            });
        }
    }

    @Test
    public void differentConfigurationsTest() throws Exception
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                        .withConfig(config -> config.set("default_keyspace_rf", "100"))
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig().set("default_keyspace_rf", "200");

            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup();

            cluster.coordinator(1).execute("CREATE KEYSPACE from_1 WITH replication = {'class': 'SimpleStrategy'}", ConsistencyLevel.ALL);
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            newInstance.coordinator().execute("CREATE KEYSPACE from_2 WITH replication = {'class': 'SimpleStrategy'}", ConsistencyLevel.ALL);
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));

            // Just like in 5.0, both nodes should see identical keyspace params (those of the coordinator).
            cluster.stream().forEach(i -> {
                i.runOnInstance(() -> {
                    KeyspaceMetadata from_1 = ClusterMetadata.current().schema.getKeyspace("from_1").getMetadata();
                    Assert.assertEquals("100", from_1.params.replication.options.get("replication_factor"));
                    KeyspaceMetadata from_2 = ClusterMetadata.current().schema.getKeyspace("from_2").getMetadata();
                    Assert.assertEquals("200", from_2.params.replication.options.get("replication_factor"));
                });
            });
        }
    }
}