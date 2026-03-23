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

package org.apache.cassandra.distributed.test.repair;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests enabling and disabling the AUTOREPAIR_ENABLE flag.
 * <p>
 * This test verifies that:
 * 1. When started with AUTOREPAIR_ENABLE=false, the auto_repair column does not appear in DESCRIBE TABLE
 * 2. After restart with AUTOREPAIR_ENABLE=true, the auto_repair column appears in DESCRIBE TABLE
 * 3. The system_distributed auto-repair tables are created after enabling
 * 4. Restarting with AUTOREPAIR_ENABLE=false after it was previously enabled fails with a deserialization error
 * <p>
 * Note: The reverse (enabled → disabled) is NOT supported and will cause the node to fail during
 * initialization due to schema incompatibility (the persisted schema contains the auto_repair column
 * that is not recognized when the property is disabled).
 */
public class AutoRepairFlagToggleTest extends TestBaseImpl
{
    @Test
    public void testDescribeWithAutoRepairSchedulerToggle() throws Exception
    {
        // Start with AUTOREPAIR_ENABLE=true
        CassandraRelevantProperties.AUTOREPAIR_ENABLE.setBoolean(true);

        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(config -> config
                                              .with(Feature.GOSSIP, Feature.NETWORK)
                                              .set("enable_materialized_views", true))
                                      .start())
        {
            // Create a test keyspace and table
            cluster.schemaChange("CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE test_ks.test_tbl (pk int PRIMARY KEY, v int)");
            cluster.schemaChange("CREATE MATERIALIZED VIEW test_ks.test_mv AS SELECT * FROM test_ks.test_tbl WHERE pk IS NOT NULL PRIMARY KEY (pk)");

            // Phase 1: With auto-repair scheduling disabled
            cluster.get(1).runOnInstance(() -> {
                org.apache.cassandra.config.DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(false);
            });

            Object[][] describeTableDisabled = cluster.coordinator(1).execute(
                    "DESCRIBE TABLE test_ks.test_tbl",
                    ConsistencyLevel.LOCAL_ONE);
            String tableStatementDisabled = (String) describeTableDisabled[0][3];
            assertFalse("DESCRIBE TABLE should NOT include auto_repair when scheduler is disabled",
                        tableStatementDisabled.contains("auto_repair"));

            Object[][] describeMvDisabled = cluster.coordinator(1).execute(
                    "DESCRIBE MATERIALIZED VIEW test_ks.test_mv",
                    ConsistencyLevel.LOCAL_ONE);
            String mvStatementDisabled = (String) describeMvDisabled[0][3];
            assertFalse("DESCRIBE MATERIALIZED VIEW should NOT include auto_repair when scheduler is disabled",
                        mvStatementDisabled.contains("auto_repair"));

            // Phase 2: Enable auto-repair scheduling
            cluster.get(1).runOnInstance(() -> {
                org.apache.cassandra.config.DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(true);
            });

            Object[][] describeTableEnabled = cluster.coordinator(1).execute(
                    "DESCRIBE TABLE test_ks.test_tbl",
                    ConsistencyLevel.LOCAL_ONE);
            String tableStatementEnabled = (String) describeTableEnabled[0][3];
            assertTrue("DESCRIBE TABLE should include auto_repair when scheduler is enabled",
                       tableStatementEnabled.contains("auto_repair"));

            Object[][] describeMvEnabled = cluster.coordinator(1).execute(
                    "DESCRIBE MATERIALIZED VIEW test_ks.test_mv",
                    ConsistencyLevel.LOCAL_ONE);
            String mvStatementEnabled = (String) describeMvEnabled[0][3];
            assertTrue("DESCRIBE MATERIALIZED VIEW should include auto_repair when scheduler is enabled",
                       mvStatementEnabled.contains("auto_repair"));

            // Phase 3: Disable auto-repair scheduling again
            cluster.get(1).runOnInstance(() -> {
                org.apache.cassandra.config.DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(false);
            });

            Object[][] describeTableDisabledAgain = cluster.coordinator(1).execute(
                    "DESCRIBE TABLE test_ks.test_tbl",
                    ConsistencyLevel.LOCAL_ONE);
            String tableStatementDisabledAgain = (String) describeTableDisabledAgain[0][3];
            assertFalse("DESCRIBE TABLE should NOT include auto_repair after disabling scheduler",
                        tableStatementDisabledAgain.contains("auto_repair"));

            Object[][] describeMvDisabledAgain = cluster.coordinator(1).execute(
                    "DESCRIBE MATERIALIZED VIEW test_ks.test_mv",
                    ConsistencyLevel.LOCAL_ONE);
            String mvStatementDisabledAgain = (String) describeMvDisabledAgain[0][3];
            assertFalse("DESCRIBE MATERIALIZED VIEW should NOT include auto_repair after disabling scheduler",
                        mvStatementDisabledAgain.contains("auto_repair"));

            // Verify table and MV are still accessible throughout
            cluster.coordinator(1).execute("INSERT INTO test_ks.test_tbl (pk, v) VALUES (1, 100)", ConsistencyLevel.ONE);
            Object[][] tableData = cluster.coordinator(1).execute("SELECT * FROM test_ks.test_tbl WHERE pk = 1", ConsistencyLevel.ONE);
            assertTrue("Table should still be accessible after scheduler toggles", tableData.length == 1);

            Object[][] mvData = cluster.coordinator(1).execute("SELECT * FROM test_ks.test_mv WHERE pk = 1", ConsistencyLevel.ONE);
            assertTrue("Materialized view should still be accessible after scheduler toggles", mvData.length == 1);
        }
    }

    @Test
    public void testEnablingAutoRepairFlag() throws Exception
    {
        // Phase 1: Start with AUTOREPAIR_ENABLE=false
        CassandraRelevantProperties.AUTOREPAIR_ENABLE.setBoolean(false);

        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(config -> config
                                              .with(Feature.GOSSIP, Feature.NETWORK))
                                      .start())
        {
            // Create a test keyspace and table
            cluster.schemaChange("CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE test_ks.test_tbl (pk int PRIMARY KEY, v int)");

            // Verify DESCRIBE TABLE does NOT show auto_repair when flag is disabled
            // DESCRIBE TABLE result columns: keyspace_name (0), type (1), name (2), create_statement (3)
            Object[][] describeResultBefore = cluster.coordinator(1).execute(
                    "DESCRIBE TABLE test_ks.test_tbl",
                    ConsistencyLevel.LOCAL_ONE);
            String createStatementBefore = (String) describeResultBefore[0][3];
            assertFalse("DESCRIBE TABLE should NOT include auto_repair when flag is disabled",
                        createStatementBefore.contains("auto_repair"));

            // Verify system_distributed auto-repair tables do NOT exist when flag is disabled
            Object[][] historyResultBefore = cluster.coordinator(1).execute(
                    "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'system_distributed' AND table_name = 'auto_repair_history'",
                    ConsistencyLevel.LOCAL_ONE);
            assertTrue("auto_repair_history table should NOT exist when flag is disabled", historyResultBefore.length == 0);

            Object[][] priorityResultBefore = cluster.coordinator(1).execute(
                    "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'system_distributed' AND table_name = 'auto_repair_priority'",
                    ConsistencyLevel.LOCAL_ONE);
            assertTrue("auto_repair_priority table should NOT exist when flag is disabled", priorityResultBefore.length == 0);

            // Phase 2: Restart with AUTOREPAIR_ENABLE=true
            cluster.get(1).nodetoolResult("drain").asserts().success();
            cluster.get(1).shutdown().get();
            CassandraRelevantProperties.AUTOREPAIR_ENABLE.setBoolean(true);
            cluster.get(1).startup();

            // Enable auto-repair scheduling so DESCRIBE TABLE will show the auto_repair property
            cluster.get(1).runOnInstance(() -> {
                org.apache.cassandra.config.DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(true);
            });

            // Verify DESCRIBE TABLE now shows auto_repair when flag is enabled
            Object[][] describeResultAfter = cluster.coordinator(1).execute(
                    "DESCRIBE TABLE test_ks.test_tbl",
                    ConsistencyLevel.LOCAL_ONE);
            String createStatementAfter = (String) describeResultAfter[0][3];
            assertTrue("DESCRIBE TABLE should include auto_repair after enabling flag",
                       createStatementAfter.contains("auto_repair"));

            // Verify the test table is still accessible
            cluster.coordinator(1).execute("INSERT INTO test_ks.test_tbl (pk, v) VALUES (1, 100)", ConsistencyLevel.ONE);
            Object[][] data = cluster.coordinator(1).execute("SELECT * FROM test_ks.test_tbl WHERE pk = 1", ConsistencyLevel.ONE);
            assertTrue("Table should still be accessible after enabling flag", data.length == 1);
        }
    }

    /**
     * Tests that restarting with AUTOREPAIR_ENABLE=false after it was previously true causes
     * "Unknown column auto_repair during deserialization" because the persisted system_schema.tables
     * SSTable references the auto_repair column which is absent from the metadata when the flag
     * is disabled. In production (disk_failure_policy=stop), this kills the JVM.
     */
    @Test
    public void testDisablingAutoRepairFlagFails() throws Exception
    {
        CassandraRelevantProperties.AUTOREPAIR_ENABLE.setBoolean(true);

        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(config -> config
                                              .with(Feature.GOSSIP, Feature.NETWORK))
                                      .start())
        {
            // Enable scheduling before creating tables so auto_repair data is written to system_schema
            cluster.get(1).runOnInstance(() -> {
                org.apache.cassandra.config.DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(true);
            });

            cluster.schemaChange("CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE test_ks.test_tbl (pk int PRIMARY KEY, v int)");

            cluster.get(1).flush("system_schema");
            cluster.get(1).nodetoolResult("drain").asserts().success();
            cluster.get(1).shutdown().get();

            CassandraRelevantProperties.AUTOREPAIR_ENABLE.setBoolean(false);
            cluster.get(1).startup();

            assertTrue("Expected 'Unknown column auto_repair during deserialization' in logs",
                       cluster.get(1).logs().grep("Unknown column auto_repair during deserialization")
                              .getResult().size() > 0);

            assertTrue("Expected user-friendly message instructing to set cassandra.autorepair.enable=true",
                       cluster.get(1).logs().grep("Set -Dcassandra.autorepair.enable=true and restart")
                              .getResult().size() > 0);
        }
    }
}
