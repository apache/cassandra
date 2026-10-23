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

package org.apache.cassandra.distributed.upgrade;

import java.util.UUID;

import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.Semver.SemverType;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.Versions;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.shared.Versions.Version;
import static org.apache.cassandra.distributed.shared.Versions.find;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests upgrading from 5.0.7 to the current version and rolling back with the UnifiedCompactionStrategy option
 * max_sstables_per_shard_factor (CASSANDRA-21615), which 5.0.7 does not know.
 * <p>
 * With node 1 upgraded and the option set on a table, node 2 on 5.0.7 keeps schema agreement and accepts
 * ALTER TABLE ... WITH, but rejects ALTER TABLE ... ADD, because that path validates the whole table definition.
 * Setting the compaction options again without the option removes it, after which node 1 is rolled back to 5.0.7
 * and both nodes accept ALTER TABLE ... ADD.
 */
public class MaxSSTablesPerShardFactorSchemaUpgradeTest extends UpgradeTestBase
{
    private static final Semver v507 = new Semver("5.0.7", SemverType.STRICT);

    @Test
    public void testMaxSSTablesPerShardFactorUpgradeAndRollback() throws Throwable
    {
        // Disable auto-repair feature to ensure no auto-repair schema changes are made
        CassandraRelevantProperties.AUTOREPAIR_ENABLE.setBoolean(false);

        Versions versions = find();
        Version from = versions.get(v507);
        Version to = AbstractCluster.CURRENT_VERSION;

        assertNotNull("5.0.7 version not available, ensure dtest-5.0.7.jar is built", from);

        try (UpgradeableCluster cluster = init(UpgradeableCluster.create(2, from,
                config -> config.with(Feature.GOSSIP, Feature.NETWORK))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v int) " +
                                 "WITH compaction = {'class': 'UnifiedCompactionStrategy'}");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, v) VALUES (1, 1)", ConsistencyLevel.ALL);

            // Verify initial schema agreement before upgrade
            UUID schemaBefore1 = cluster.get(1).schemaVersion();
            UUID schemaBefore2 = cluster.get(2).schemaVersion();
            assertEquals("Schema versions should match before upgrade", schemaBefore1, schemaBefore2);

            // Upgrade only node 1 to current version
            cluster.get(1).shutdown().get();
            cluster.get(1).setVersion(to);
            cluster.get(1).startup();

            // Wait for schema to settle
            Thread.sleep(5000);

            // Verify schema agreement after upgrade
            UUID schemaAfter1 = cluster.get(1).schemaVersion();
            UUID schemaAfter2 = cluster.get(2).schemaVersion();
            assertNotNull("Node 1 schema version should not be null after upgrade", schemaAfter1);
            assertNotNull("Node 2 schema version should not be null after upgrade", schemaAfter2);
            assertEquals("Schema versions should match between upgraded and non-upgraded nodes", schemaAfter1, schemaAfter2);

            // Set the option through the upgraded node. Node 2 on 5.0.7 applies the schema without validating it
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl WITH compaction = " +
                                 "{'class': 'UnifiedCompactionStrategy', 'max_sstables_per_shard_factor': '2'}");
            UUID schemaWithOption1 = cluster.get(1).schemaVersion();
            UUID schemaWithOption2 = cluster.get(2).schemaVersion();
            assertNotNull("Node 1 schema version should not be null with the option set", schemaWithOption1);
            assertNotNull("Node 2 schema version should not be null with the option set", schemaWithOption2);
            assertEquals("Schema versions should match with the option set", schemaWithOption1, schemaWithOption2);

            // Writes coordinated by node 2 and flushes on both nodes work with the option in the schema
            cluster.coordinator(2).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, v) VALUES (2, 2)", ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).flush(KEYSPACE);

            // Verify data is readable from both nodes with the option set
            assertEquals("Data should be readable from node 1 with the option set", 2,
                         cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL).length);
            assertEquals("Data should be readable from node 2 with the option set", 2,
                         cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL).length);

            // ALTER TABLE ... WITH on the 5.0.7 node validates only the options it is given
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl WITH comment = 'mixed versions'", false, cluster.get(2));

            // ALTER TABLE ... ADD on the 5.0.7 node validates the whole table and rejects the unknown option
            Assertions.assertThatThrownBy(() -> cluster.coordinator(2).execute("ALTER TABLE " + KEYSPACE + ".tbl ADD w int", ConsistencyLevel.ALL))
                      .hasMessageContaining("[max_sstables_per_shard_factor] are not understood by UnifiedCompactionStrategy");

            // The upgraded node accepts it
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD w int");

            // Remove the option from the 5.0.7 node by setting the compaction options again without it
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl WITH compaction = {'class': 'UnifiedCompactionStrategy'}",
                                 false, cluster.get(2));

            // Rollback: downgrade node 1 back to 5.0.7
            cluster.get(1).shutdown().get();
            cluster.get(1).setVersion(from);
            cluster.get(1).startup();

            // Wait for schema to settle
            Thread.sleep(5000);

            // Verify schema agreement after rollback
            UUID schemaRollback1 = cluster.get(1).schemaVersion();
            UUID schemaRollback2 = cluster.get(2).schemaVersion();
            assertNotNull("Node 1 schema version should not be null after rollback", schemaRollback1);
            assertNotNull("Node 2 schema version should not be null after rollback", schemaRollback2);
            assertEquals("Schema versions should match after rollback to 5.0.7", schemaRollback1, schemaRollback2);

            // Without the option both 5.0.7 nodes accept ALTER TABLE ... ADD again
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD x int", false, cluster.get(1));
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD y int", false, cluster.get(2));

            // Verify data is still readable after rollback
            assertEquals("Data should be readable from node 1 after rollback", 2,
                         cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL).length);
            assertEquals("Data should be readable from node 2 after rollback", 2,
                         cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL).length);
        }
    }
}
