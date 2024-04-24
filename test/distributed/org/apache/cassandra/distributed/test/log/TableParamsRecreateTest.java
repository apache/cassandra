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
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableId;
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
            cluster.coordinator(1).execute("CREATE TABLE " + KEYSPACE + ".before_bounce (id int PRIMARY KEY, value text);", ConsistencyLevel.ALL);
            cluster.get(1).shutdown().get();
            cluster.get(1).config()
                   .set("default_compaction", Map.of("class_name", "SizeTieredCompactionStrategy",
                                                     "parameters", Map.of(
                   "min_threshold", "32",
                   "max_threshold", "128")));;

            cluster.get(1).startup();
            cluster.coordinator(1).execute("CREATE TABLE " + KEYSPACE + ".after_bounce (id int PRIMARY KEY, value text);", ConsistencyLevel.ALL);

            // Tables created before the bounce should preserve the initial default value. Those created after updating
            // should take the new value from config.
            cluster.stream().forEach(i -> {
                i.runOnInstance(() -> {
                    CompactionParams before_bounce = ClusterMetadata.current().schema.getKeyspace(KEYSPACE).getMetadata().tables.get("before_bounce").get().params.compaction;
                    Assert.assertEquals("16", before_bounce.options().get("min_threshold"));
                    Assert.assertEquals("64", before_bounce.options().get("max_threshold"));
                    CompactionParams after_bounce = ClusterMetadata.current().schema.getKeyspace(KEYSPACE).getMetadata().tables.get("after_bounce").get().params.compaction;
                    Assert.assertEquals("32", after_bounce.options().get("min_threshold"));
                    Assert.assertEquals("128", after_bounce.options().get("max_threshold"));
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

            // Just like in 5.0, both nodes should see identical table params (those from the coordinator)
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
    public void laggingNodeTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start(),1))
        {
            IMessageFilters.Filter filter = cluster.filters().allVerbs().from(1).to(2).drop();

            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute(String.format("CREATE TABLE %s.tbl%d (id int PRIMARY KEY, value text);", KEYSPACE, i), ConsistencyLevel.ALL);
                int finalI = i;
                cluster.get(1).runOnInstance(() -> {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    Assert.assertEquals(TableId.fromLong(metadata.epoch.getEpoch() - 1),
                                        metadata.schema.getKeyspace(KEYSPACE).getMetadata().tables.get("tbl" + finalI).get().id);
                });
            }

            int expectedEpoch = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch()).intValue();
            int laggingEpoch = cluster.get(2).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch()).intValue();
            Assert.assertTrue(String.format("%d should be less than %s", laggingEpoch, expectedEpoch), laggingEpoch < expectedEpoch);

            filter.off();
            cluster.coordinator(2).execute(String.format("CREATE TABLE %s.tbl%d (id int PRIMARY KEY, value text);", KEYSPACE, 10), ConsistencyLevel.ALL);

            cluster.get(2).runOnInstance(() -> {
                Assert.assertEquals(TableId.fromLong(expectedEpoch), ClusterMetadata.current().schema.getKeyspace(KEYSPACE).getMetadata().tables.get("tbl10").get().id);
            });

            cluster.coordinator(2).execute(String.format("CREATE TABLE %s.tbl%d (id int PRIMARY KEY, value text) WITH id = %s;", KEYSPACE, 11, new UUID(11, 11)),
                                                         ConsistencyLevel.ALL);

            cluster.get(2).runOnInstance(() -> {
                Assert.assertEquals(TableId.fromUUID(new UUID(11, 11)),
                                    ClusterMetadata.current().schema.getKeyspace(KEYSPACE).getMetadata().tables.get("tbl11").get().id);
            });
        }
    }

    @Test
    public void compactStorageTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start(),1))
        {
            cluster.coordinator(1).execute(String.format("CREATE TABLE %s.tbl (id int PRIMARY KEY, value text) WITH COMPACT STORAGE;", KEYSPACE), ConsistencyLevel.ALL);
        }
    }
}