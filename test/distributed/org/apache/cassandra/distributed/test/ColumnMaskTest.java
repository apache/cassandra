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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests for dynamic data masking.
 */
public class ColumnMaskTest extends TestBaseImpl
{
    private static final String SELECT = withKeyspace("SELECT * FROM %s.t");

    /**
     * Tests that column masks are propagated to all nodes in the cluster.
     */
    @Test
    public void testMaskPropagation() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v text MASKED WITH DEFAULT) WITH read_repair='NONE'"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.t(k, v) VALUES (1, 'secret1')"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.t(k, v) VALUES (2, 'secret2')"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.t(k, v) VALUES (3, 'secret3')"));

            assertRows(cluster.get(1).executeInternal(SELECT), row(1, "****"));
            assertRows(cluster.get(2).executeInternal(SELECT), row(2, "****"));
            assertRows(cluster.get(3).executeInternal(SELECT), row(3, "****"));
            assertRowsInAllCoordinators(cluster, row(1, "****"), row(2, "****"), row(3, "****"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v DROP MASKED"));
            assertRows(cluster.get(1).executeInternal(SELECT), row(1, "secret1"));
            assertRows(cluster.get(2).executeInternal(SELECT), row(2, "secret2"));
            assertRows(cluster.get(3).executeInternal(SELECT), row(3, "secret3"));
            assertRowsInAllCoordinators(cluster, row(1, "secret1"), row(2, "secret2"), row(3, "secret3"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v MASKED WITH mask_inner(null, 1)"));
            assertRows(cluster.get(1).executeInternal(SELECT), row(1, "******1"));
            assertRows(cluster.get(2).executeInternal(SELECT), row(2, "******2"));
            assertRows(cluster.get(3).executeInternal(SELECT), row(3, "******3"));
            assertRowsInAllCoordinators(cluster, row(1, "******1"), row(2, "******2"), row(3, "******3"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v MASKED WITH mask_inner(3, null)"));
            assertRows(cluster.get(1).executeInternal(SELECT), row(1, "sec****"));
            assertRows(cluster.get(2).executeInternal(SELECT), row(2, "sec****"));
            assertRows(cluster.get(3).executeInternal(SELECT), row(3, "sec****"));
            assertRowsInAllCoordinators(cluster, row(1, "sec****"), row(2, "sec****"), row(3, "sec****"));
        }
    }

    private static void assertRowsInAllCoordinators(Cluster cluster, Object[]... expectedRows)
    {
        for (int i = 1; i < cluster.size(); i++)
        {
            assertRows(cluster.coordinator(i).execute(SELECT, ALL), expectedRows);
        }
    }

    /**
     * Tests that column masks are properly loaded at startup.
     */
    @Test
    public void testMaskLoading() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build().withNodes(1).start()))
        {
            IInvokableInstance node = cluster.get(1);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v text MASKED WITH DEFAULT) "));
            node.executeInternal(withKeyspace("INSERT INTO %s.t(k, v) VALUES (1, 'secret1')"));
            node.executeInternal(withKeyspace("INSERT INTO %s.t(k, v) VALUES (2, 'secret2')"));

            assertRowsWithRestart(node, row(1, "****"), row(2, "****"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v DROP MASKED"));
            assertRowsWithRestart(node, row(1, "secret1"), row(2, "secret2"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v MASKED WITH mask_inner(null, 1)"));
            assertRowsWithRestart(node, row(1, "******1"), row(2, "******2"));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ALTER v MASKED WITH mask_inner(3, null)"));
            assertRowsWithRestart(node, row(1, "sec****"), row(2, "sec****"));
        }
    }

    private static void assertRowsWithRestart(IInvokableInstance node, Object[]... expectedRows) throws Throwable
    {
        // test querying with in-memory column definitions
        assertRows(node.executeInternal(SELECT), expectedRows);

        // restart the nodes to reload the column definitions from disk
        node.shutdown().get();
        node.startup();

        // test querying with the column definitions loaded from disk
        assertRows(node.executeInternal(SELECT), expectedRows);
    }
}
