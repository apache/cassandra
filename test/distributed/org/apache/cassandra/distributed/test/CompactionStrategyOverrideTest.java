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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;

import static org.apache.cassandra.config.CassandraRelevantProperties.OVERRIDE_COMPACTION_ENTITIES;
import static org.apache.cassandra.config.CassandraRelevantProperties.OVERRIDE_COMPACTION_PARAMS;

public class CompactionStrategyOverrideTest extends TestBaseImpl
{
    private static final String OVERRIDE_PARAMS = "{\"class\":\"org.apache.cassandra.db.compaction.LeveledCompactionStrategy\",\"sstable_size_in_mb\":\"512\"}";

    @Test
    public void testCompactionStrategyOverrideOnRestart() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                         .set(Constants.KEY_DTEST_FULL_STARTUP, true))
                                             .start()))
        {
            cluster.coordinator(1).execute("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("CREATE TABLE ks1.tbl1 (id int PRIMARY KEY, value text)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("CREATE TABLE ks1.tbl2 (id int PRIMARY KEY, value text)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("CREATE TABLE ks2.tbl1 (id int PRIMARY KEY, value text)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("CREATE TABLE ks2.tbl2 (id int PRIMARY KEY, value text)", ConsistencyLevel.ALL);

            // Verify all tables start with the default SizeTieredCompactionStrategy
            cluster.get(1).runOnInstance(() -> {
                for (String ks : new String[]{ "ks1", "ks2" })
                    for (String tbl : new String[]{ "tbl1", "tbl2" })
                        Assert.assertEquals("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                                            Keyspace.open(ks).getColumnFamilyStore(tbl).getCompactionParameters().get("class"));
            });

            // Shut down, set override properties, and restart so CassandraDaemon.setup() applies them
            cluster.get(1).shutdown().get();

            OVERRIDE_COMPACTION_ENTITIES.setString("ks1.tbl1,ks1,ks2.tbl2");
            OVERRIDE_COMPACTION_PARAMS.setString(OVERRIDE_PARAMS);

            cluster.get(1).startup();

            cluster.get(1).runOnInstance(() -> {
                // ks1 was listed as a whole keyspace (ks1.tbl1,ks1 -> ks1 overrides), so both tables should be overridden
                Map<String, String> ks1tbl1 = Keyspace.open("ks1").getColumnFamilyStore("tbl1").getCompactionParameters();
                Assert.assertEquals("org.apache.cassandra.db.compaction.LeveledCompactionStrategy", ks1tbl1.get("class"));
                Assert.assertEquals("512", ks1tbl1.get("sstable_size_in_mb"));

                Map<String, String> ks1tbl2 = Keyspace.open("ks1").getColumnFamilyStore("tbl2").getCompactionParameters();
                Assert.assertEquals("org.apache.cassandra.db.compaction.LeveledCompactionStrategy", ks1tbl2.get("class"));
                Assert.assertEquals("512", ks1tbl2.get("sstable_size_in_mb"));

                // ks2.tbl2 was explicitly listed, so it should be overridden
                Map<String, String> ks2tbl2 = Keyspace.open("ks2").getColumnFamilyStore("tbl2").getCompactionParameters();
                Assert.assertEquals("org.apache.cassandra.db.compaction.LeveledCompactionStrategy", ks2tbl2.get("class"));
                Assert.assertEquals("512", ks2tbl2.get("sstable_size_in_mb"));

                // ks2.tbl1 was not listed, so it should retain the default SizeTieredCompactionStrategy
                Map<String, String> ks2tbl1 = Keyspace.open("ks2").getColumnFamilyStore("tbl1").getCompactionParameters();
                Assert.assertEquals("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", ks2tbl1.get("class"));
                Assert.assertNull(ks2tbl1.get("sstable_size_in_mb"));
            });

            System.clearProperty(OVERRIDE_COMPACTION_ENTITIES.getKey());
            System.clearProperty(OVERRIDE_COMPACTION_PARAMS.getKey());
        }
    }
}
