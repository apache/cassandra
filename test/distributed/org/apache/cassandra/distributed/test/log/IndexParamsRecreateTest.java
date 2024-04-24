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
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.tcm.ClusterMetadata;

public class IndexParamsRecreateTest extends TestBaseImpl
{
    protected static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT, v3 TEXT) WITH compaction = " +
                                                          "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    protected static final String CREATE_DEFAULT_INDEX_TEMPLATE = "CREATE INDEX IF NOT EXISTS default_idx ON %s(%s)";
    protected static final String CREATE_SAI_INDEX_TEMPLATE = "CREATE INDEX IF NOT EXISTS sai_idx ON %s(%s) USING 'sai'";
    protected static final String CREATE_LEGACY_INDEX_TEMPLATE = "CREATE INDEX IF NOT EXISTS legacy_idx ON %s(%s) USING 'legacy_local_table'";

    @Test
    public void bounceTest() throws Exception
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> config.set("default_secondary_index", "legacy_local_table")
                                                                    .set("default_secondary_index_enabled", "true"))
                                        .start())
        {
            cluster.coordinator(1).execute("CREATE KEYSPACE before_bounce WITH replication = {'class': 'SimpleStrategy'}", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_TABLE_TEMPLATE, "before_bounce.tbl1"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_DEFAULT_INDEX_TEMPLATE, "before_bounce.tbl1", "v1"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_LEGACY_INDEX_TEMPLATE, "before_bounce.tbl1", "v2"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_SAI_INDEX_TEMPLATE, "before_bounce.tbl1", "v3"), ConsistencyLevel.ALL);

            cluster.get(1).shutdown().get();
            cluster.get(1).config()
                   .set("default_secondary_index", "sai")
                   .set("default_secondary_index_enabled", "true");

            cluster.get(1).startup();
            cluster.coordinator(1).execute("CREATE KEYSPACE after_bounce WITH replication = {'class': 'SimpleStrategy'}", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_TABLE_TEMPLATE, "after_bounce.tbl1"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_DEFAULT_INDEX_TEMPLATE, "after_bounce.tbl1", "v1"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_LEGACY_INDEX_TEMPLATE, "after_bounce.tbl1", "v2"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_SAI_INDEX_TEMPLATE, "after_bounce.tbl1", "v3"), ConsistencyLevel.ALL);

            // Where an index class was explicitly specified with USING, changing the default should have no effect.
            // The index created using the default implementation before the bounce should preserve its original type
            // while the one created after bounce should use the new default.
            cluster.stream().forEach(i -> {
                i.runOnInstance(() -> {
                    Indexes before_bounce = ClusterMetadata.current().schema.getKeyspace("before_bounce")
                                                                            .getMetadata()
                                                                            .getTableNullable("tbl1")
                                            .indexes;
                    assertIndex(before_bounce.get("legacy_idx").get(), IndexMetadata.Kind.COMPOSITES, CassandraIndex.class);
                    assertIndex(before_bounce.get("sai_idx").get(), IndexMetadata.Kind.CUSTOM, StorageAttachedIndex.class);
                    // the original default was legacy 2i
                    assertIndex(before_bounce.get("default_idx").get(), IndexMetadata.Kind.COMPOSITES, CassandraIndex.class);

                    Indexes after_bounce = ClusterMetadata.current().schema.getKeyspace("after_bounce")
                                                                           .getMetadata()
                                                                           .getTableNullable("tbl1")
                                           .indexes;
                    assertIndex(after_bounce.get("legacy_idx").get(), IndexMetadata.Kind.COMPOSITES, CassandraIndex.class);
                    assertIndex(after_bounce.get("sai_idx").get(), IndexMetadata.Kind.CUSTOM, StorageAttachedIndex.class);
                    // the updated default is sai
                    assertIndex(after_bounce.get("default_idx").get(), IndexMetadata.Kind.CUSTOM, StorageAttachedIndex.class);
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
                                        .withConfig(config -> config.set("default_secondary_index", "legacy_local_table")
                                                                    .set("default_secondary_index_enabled", "true"))
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("default_secondary_index", "sai")
                                            .set("default_secondary_index_enabled", "true");

            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup();

            cluster.coordinator(1).execute("CREATE KEYSPACE from_1 WITH replication = {'class': 'SimpleStrategy'}", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_TABLE_TEMPLATE, "from_1.tbl1"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_DEFAULT_INDEX_TEMPLATE, "from_1.tbl1", "v1"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_LEGACY_INDEX_TEMPLATE, "from_1.tbl1", "v2"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format(CREATE_SAI_INDEX_TEMPLATE, "from_1.tbl1", "v3"), ConsistencyLevel.ALL);

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            newInstance.coordinator().execute("CREATE KEYSPACE from_2 WITH replication = {'class': 'SimpleStrategy'}", ConsistencyLevel.ALL);
            newInstance.coordinator().execute(String.format(CREATE_TABLE_TEMPLATE, "from_2.tbl1"), ConsistencyLevel.ALL);
            newInstance.coordinator().execute(String.format(CREATE_DEFAULT_INDEX_TEMPLATE, "from_2.tbl1", "v1"), ConsistencyLevel.ALL);
            newInstance.coordinator().execute(String.format(CREATE_LEGACY_INDEX_TEMPLATE, "from_2.tbl1", "v2"), ConsistencyLevel.ALL);
            newInstance.coordinator().execute(String.format(CREATE_SAI_INDEX_TEMPLATE, "from_2.tbl1", "v3"), ConsistencyLevel.ALL);

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));

            // Just like in 5.0, both nodes should see identical index params (those of the coordinator).
            cluster.stream().forEach(i -> {
                i.runOnInstance(() -> {
                    Indexes from_1 = ClusterMetadata.current().schema.getKeyspace("from_1")
                                                                     .getMetadata()
                                                                     .getTableNullable("tbl1")
                                     .indexes;
                    Assert.assertEquals(IndexMetadata.Kind.COMPOSITES, from_1.get("default_idx").get().kind);
                    Assert.assertEquals(IndexMetadata.Kind.COMPOSITES, from_1.get("legacy_idx").get().kind);
                    Assert.assertEquals(IndexMetadata.Kind.CUSTOM, from_1.get("sai_idx").get().kind);
                    assertIndex(from_1.get("legacy_idx").get(), IndexMetadata.Kind.COMPOSITES, CassandraIndex.class);
                    assertIndex(from_1.get("sai_idx").get(), IndexMetadata.Kind.CUSTOM, StorageAttachedIndex.class);
                    // Node1's default is legacy 2i
                    assertIndex(from_1.get("default_idx").get(), IndexMetadata.Kind.COMPOSITES, CassandraIndex.class);

                    Indexes from_2 = ClusterMetadata.current().schema.getKeyspace("from_2")
                                                                     .getMetadata()
                                                                     .getTableNullable("tbl1")
                                     .indexes;
                    assertIndex(from_2.get("legacy_idx").get(), IndexMetadata.Kind.COMPOSITES, CassandraIndex.class);
                    assertIndex(from_2.get("sai_idx").get(), IndexMetadata.Kind.CUSTOM, StorageAttachedIndex.class);
                    // Node2's default is sai
                    assertIndex(from_2.get("default_idx").get(), IndexMetadata.Kind.CUSTOM, StorageAttachedIndex.class);
                });
            });
        }
    }

    private static void assertIndex(IndexMetadata index, IndexMetadata.Kind expectedKind, Class expectedClass)
    {
        Assert.assertEquals(expectedKind, index.kind);
        Assert.assertEquals(expectedClass.getName(), index.getIndexClassName());
    }
}