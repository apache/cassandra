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

import static org.apache.cassandra.distributed.shared.Versions.Version;
import static org.apache.cassandra.distributed.shared.Versions.find;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests that upgrading from 5.0.7 (where auto-repair is not included) to the current version
 * with AUTO_REPAIR_ENABLE=false maintains schema agreement between nodes.
 * <p>
 * This test verifies that when the auto-repair feature is disabled via the JVM property,
 * the schema version remains consistent between upgraded and non-upgraded nodes,
 * ensuring no schema disagreement occurs due to conditional auto-repair schema changes.
 */
public class AutoRepairDisabledSchemaUpgradeTest extends UpgradeTestBase
{
    private static final Semver v507 = new Semver("5.0.7", SemverType.STRICT);

    @Test
    public void testSchemaAgreementWithAutoRepairDisabled() throws Throwable
    {
        // Disable auto-repair feature to ensure no auto-repair schema changes are made
        CassandraRelevantProperties.AUTOREPAIR_ENABLE.setBoolean(false);

        Versions versions = find();
        Version from = versions.get(v507);
        Version to = AbstractCluster.CURRENT_VERSION;

        assertNotNull("5.0.7 version not available - ensure dtest-5.0.7.jar is built", from);

        try (UpgradeableCluster cluster = init(UpgradeableCluster.create(2, from,
                config -> config.with(Feature.GOSSIP, Feature.NETWORK))))
        {
            // Create a simple table to ensure schema is propagated
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v int)");
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

            // Verify data is still readable from both nodes
            Object[][] result1 = cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL);
            Object[][] result2 = cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL);
            assertEquals("Data should be readable from node 1", 1, result1.length);
            assertEquals("Data should be readable from node 2", 1, result2.length);

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

            // Verify data is still readable after rollback
            Object[][] resultRollback1 = cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL);
            Object[][] resultRollback2 = cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL);
            assertEquals("Data should be readable from node 1 after rollback", 1, resultRollback1.length);
            assertEquals("Data should be readable from node 2 after rollback", 1, resultRollback2.length);
        }
    }
}
