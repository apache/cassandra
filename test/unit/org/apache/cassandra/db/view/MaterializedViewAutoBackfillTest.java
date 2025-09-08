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

package org.apache.cassandra.db.view;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ViewAbstractTest;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for materialized view auto-backfill configuration feature.
 */
public class MaterializedViewAutoBackfillTest extends ViewAbstractTest
{
    private boolean originalAutoBackfillSetting;

    @Before
    @Override
    public void beforeTest() throws Throwable
    {
        super.beforeTest();
        // Store original setting to restore later
        originalAutoBackfillSetting = DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled();
    }

    @After
    public void afterTest()
    {
        // Restore original setting
        DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(originalAutoBackfillSetting);
    }

    @Test
    public void testViewCreationWithAutoBackfillEnabled() throws Throwable
    {
        // Ensure auto-backfill is enabled
        DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(true);

        // Create base table and insert data
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 100)");
        execute("INSERT INTO %s (k, v) VALUES (2, 200)");

        // Create materialized view - should trigger backfill
        createView("test_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
        
        // Verify data is in the view (this confirms backfill worked)
        assertRowCount(execute("SELECT * FROM test_view"), 2);
    }

    @Test
    public void testViewCreationWithAutoBackfillDisabled() throws Throwable
    {
        // Disable auto-backfill
        DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(false);

        // Create base table and insert data
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 100)");
        execute("INSERT INTO %s (k, v) VALUES (2, 200)");

        // Create materialized view - should NOT trigger backfill
        createView("test_view_no_backfill", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                           "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        String keyspaceName = baseCfs.keyspace.getName();
        
        // The view should be marked as built (without actual data backfill)
        assertTrue("View should be marked as built even without backfill",
                   SystemKeyspace.isViewBuilt(keyspaceName, "test_view_no_backfill"));

        // Verify no data is in the view initially (backfill was skipped)
        assertRowCount(execute("SELECT * FROM test_view_no_backfill"), 0);

        // But new inserts should still work (ongoing updates)
        execute("INSERT INTO %s (k, v) VALUES (3, 300)");
        
        // The new insert should appear in the view
        assertRowCount(execute("SELECT * FROM test_view_no_backfill"), 1);
        assertRows(execute("SELECT * FROM test_view_no_backfill"), row(300, 3));
    }

    @Test
    public void testViewManagerReloadBehavior() throws Throwable
    {
        // Test that ViewManager.reload() respects the auto-backfill setting
        DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(false);

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 100)");

        // Create view with auto-backfill disabled
        createView("test_reload_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                      "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        String keyspaceName = baseCfs.keyspace.getName();
        ViewManager viewManager = baseCfs.keyspace.viewManager;

        // Verify view is marked as built but has no data
        assertTrue("View should be marked as built", 
                   SystemKeyspace.isViewBuilt(keyspaceName, "test_reload_view"));
        assertRowCount(execute("SELECT * FROM test_reload_view"), 0);

        // Now enable auto-backfill and trigger reload
        DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(true);
        
        // Reload should not trigger backfill for already-built views
        viewManager.reload(true);
        
        // View should still be marked as built and have no historical data
        assertTrue("View should still be marked as built after reload", 
                   SystemKeyspace.isViewBuilt(keyspaceName, "test_reload_view"));
        assertRowCount(execute("SELECT * FROM test_reload_view"), 0);

        // But new data should still be indexed
        execute("INSERT INTO %s (k, v) VALUES (2, 200)");
        assertRowCount(execute("SELECT * FROM test_reload_view"), 1);
    }

    @Test
    public void testMultipleViewsWithMixedSettings() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, x int)");
        execute("INSERT INTO %s (k, v, x) VALUES (1, 100, 1000)");
        execute("INSERT INTO %s (k, v, x) VALUES (2, 200, 2000)");

        // Create first view with auto-backfill enabled
        DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(true);
        createView("view_with_backfill", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                        "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");

        // Verify first view has data
        assertRowCount(execute("SELECT * FROM view_with_backfill"), 2);

        // Create second view with auto-backfill disabled
        DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(false);
        createView("view_without_backfill", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                           "WHERE k IS NOT NULL AND x IS NOT NULL PRIMARY KEY (x, k)");

        // Verify second view has no historical data
        assertRowCount(execute("SELECT * FROM view_without_backfill"), 0);

        // Add new data - both views should get it
        execute("INSERT INTO %s (k, v, x) VALUES (3, 300, 3000)");
        
        assertRowCount(execute("SELECT * FROM view_with_backfill"), 3);
        assertRowCount(execute("SELECT * FROM view_without_backfill"), 1);
        assertRows(execute("SELECT * FROM view_without_backfill"), row(3000, 3, 300));
    }

    @Test
    public void testConfigurationPersistence() throws Throwable
    {
        // Test that configuration changes persist across operations
        DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(false);
        
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 100)");

        // Configuration should still be false
        assertFalse("Configuration should persist", 
                    DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled());

        createView("persistent_test_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                          "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");

        // View should be created without backfill
        assertRowCount(execute("SELECT * FROM persistent_test_view"), 0);

        // Change configuration
        DatabaseDescriptor.setMaterializedViewAutoBackfillEnabled(true);
        assertTrue("Configuration should be updated", 
                   DatabaseDescriptor.getMaterializedViewAutoBackfillEnabled());

        // Create another view - this one should have backfill
        createView("persistent_test_view2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                           "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (k, v)");

        // This view should have the existing data
        assertRowCount(execute("SELECT * FROM persistent_test_view2"), 1);
        assertRows(execute("SELECT * FROM persistent_test_view2"), row(1, 100));
    }
}
