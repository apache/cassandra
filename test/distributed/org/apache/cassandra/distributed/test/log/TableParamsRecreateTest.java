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

import java.util.Map;

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
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.tcm.ClusterMetadata;

public class TableParamsRecreateTest extends TestBaseImpl
{
    @Test
    public void bounceTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(config -> config.set("default_compaction", Map.of(
                                             "class_name", "SizeTieredCompactionStrategy",
                                             "parameters", Map.of(
                                             "min_threshold", "16",
                                             "max_threshold", "64"))))
                                             .start()))
        {
            cluster.coordinator(1).execute("CREATE TABLE " + KEYSPACE + ".from_1 (id int PRIMARY KEY, value text);", ConsistencyLevel.ALL);
            cluster.get(1).shutdown().get();
            cluster.get(1).config()
                   .set("default_compaction", Map.of("class_name", "SizeTieredCompactionStrategy",
                                                     "parameters", Map.of(
                   "min_threshold", "32",
                   "max_threshold", "128")));;

            cluster.get(1).startup();
            cluster.coordinator(1).execute("CREATE TABLE " + KEYSPACE + ".from_2 (id int PRIMARY KEY, value text);", ConsistencyLevel.ALL);

            // Just like in 5.0, table created before the bounce, should preserve its default, and one after bonce - is own
            cluster.stream().forEach(i -> {
                i.runOnInstance(() -> {
                    CompactionParams from_1 = ClusterMetadata.current().schema.getKeyspace(KEYSPACE).getMetadata().tables.get("from_1").get().params.compaction;
                    Assert.assertEquals("16", from_1.options().get("min_threshold"));
                    Assert.assertEquals("64", from_1.options().get("max_threshold"));
                    CompactionParams from_2 = ClusterMetadata.current().schema.getKeyspace(KEYSPACE).getMetadata().tables.get("from_2").get().params.compaction;
                    Assert.assertEquals("32", from_2.options().get("min_threshold"));
                    Assert.assertEquals("128", from_2.options().get("max_threshold"));
                });
            });
        }
    }

    @Test
    public void differentConfigurationsTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                                             .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                             .withConfig(config -> config.set("default_compaction", Map.of(
                                             "class_name", "SizeTieredCompactionStrategy",
                                             "parameters", Map.of(
                                             "min_threshold", "16",
                                             "max_threshold", "64"))))
                                             .start(),
                                    1))
        {
            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("default_compaction", Map.of("class_name", "SizeTieredCompactionStrategy",
                                                                              "parameters", Map.of(
                                            "min_threshold", "32",
                                            "max_threshold", "128")));

            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup();

            cluster.coordinator(1).execute("CREATE TABLE " + KEYSPACE + ".from_1 (id int PRIMARY KEY, value text);", ConsistencyLevel.ALL);
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            newInstance.coordinator().execute("CREATE TABLE " + KEYSPACE + ".from_2 (id int PRIMARY KEY, value text);", ConsistencyLevel.ALL);
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));

            // Just like in 5.0, both nodes should see idential table params
            cluster.stream().forEach(i -> {
                i.runOnInstance(() -> {
                    CompactionParams from_1 = ClusterMetadata.current().schema.getKeyspace(KEYSPACE).getMetadata().tables.get("from_1").get().params.compaction;
                    Assert.assertEquals("16", from_1.options().get("min_threshold"));
                    Assert.assertEquals("64", from_1.options().get("max_threshold"));
                    CompactionParams from_2 = ClusterMetadata.current().schema.getKeyspace(KEYSPACE).getMetadata().tables.get("from_2").get().params.compaction;
                    Assert.assertEquals("32", from_2.options().get("min_threshold"));
                    Assert.assertEquals("128", from_2.options().get("max_threshold"));
                });
            });
        }
    }
}