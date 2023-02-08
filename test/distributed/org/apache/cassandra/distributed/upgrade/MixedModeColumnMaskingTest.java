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

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests that dynamic data masking (DDM) functions can be attached to table columns during a rolling upgrade involving
 * nodes that don't include DDM.
 */
public class MixedModeColumnMaskingTest extends UpgradeTestBase
{
    @Test
    public void testColumnMasking() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1, 2)
        .upgradesToCurrentFrom(v30)
        .withConfig(config -> config.with(GOSSIP))
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int)"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t(k, v) VALUES (0, 7)"), ALL);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node == 1)
            {
                ICoordinator coordinator = cluster.coordinator(1);

                // create table with masked column
                assertFails(coordinator,
                            "CREATE TABLE %s.t1 (k int PRIMARY KEY, v int MASKED WITH DEFAULT)",
                            "Cannot create a table with data masking functions during rolling upgrade");

                // mask existing column
                assertFails(coordinator,
                            "ALTER TABLE %s.t ALTER v MASKED WITH DEFAULT",
                            "Cannot add masking function to column during rolling upgrade");
                assertColumnIsNotMasked(cluster, "t", "v");

                // unmask existing column
                assertFails(coordinator,
                            "ALTER TABLE %s.t ALTER v DROP MASKED",
                            "Cannot remove masking function from column during rolling upgrade");
                assertColumnIsNotMasked(cluster, "t", "v");

                // add new masked column
                assertFails(coordinator,
                            "ALTER TABLE %s.t ADD v2 int MASKED WITH DEFAULT",
                            "Cannot add column with masking function during rolling upgrade");
                assertColumnIsNotMasked(cluster, "t", "v");
            }
        }).runAfterClusterUpgrade(cluster -> {

            // create table with masked column
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t2 (k int PRIMARY KEY, v int MASKED WITH DEFAULT)"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t2(k, v) VALUES (0, 7)"), ALL);
            assertColumnIsMasked(cluster, "t2", "v");

            // unmask existing column
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t2 ALTER v DROP MASKED"));
            assertColumnIsNotMasked(cluster, "t2", "v");

            // mask existing column
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t2 ALTER v MASKED WITH DEFAULT"));
            assertColumnIsMasked(cluster, "t2", "v");

            // add new masked column
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ADD v2 int MASKED WITH DEFAULT"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t(k, v2) VALUES (0, 7)"), ALL);
            assertColumnIsMasked(cluster, "t", "v2");

            // unmask the new column
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v2 DROP MASKED"));
            assertColumnIsNotMasked(cluster, "t", "v2");
        })
        .run();
    }

    private static void assertFails(ICoordinator coordinator, String schemaQuery, String expectedMessage)
    {
        Assertions.assertThatThrownBy(() -> coordinator.execute(withKeyspace(schemaQuery), ALL))
                  .hasMessageContaining(expectedMessage);
    }

    private static void assertColumnIsMasked(UpgradeableCluster cluster, String table, String column)
    {
        assertRows(getColumnMetadata(cluster, table, column), row(column, SchemaConstants.SYSTEM_KEYSPACE_NAME));
    }

    private static void assertColumnIsNotMasked(UpgradeableCluster cluster, String table, String column)
    {
        assertRows(getColumnMetadata(cluster, table, column), row(column, null));
    }

    private static Object[][] getColumnMetadata(UpgradeableCluster cluster, String table, String column)
    {
        String query = format("SELECT column_name, mask_keyspace FROM %s.%s " +
                              "WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                              SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMNS);
        return cluster.coordinator(1).execute(query, ALL, KEYSPACE, table, column);
    }
}
