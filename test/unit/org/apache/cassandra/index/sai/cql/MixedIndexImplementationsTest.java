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
package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;

import static org.junit.Assert.assertNotNull;

/**
 * Tests behaviour when there are non-storage-attached indexes in the table.
 */
public class MixedIndexImplementationsTest extends SAITester
{
    /**
     * Tests that storage-attached indexes can be dropped when there are other indexes in the same table, and vice versa.
     */
    @Test
    public void shouldDropOtherIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");

        String ossIndex = createIndex("CREATE INDEX ON %s(v1)");
        String ndiIndex = createIndex(
                String.format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", StorageAttachedIndex.class.getName()));

        // drop non-storage-attached index when a SAI index exists
        dropIndex("DROP INDEX %s." + ossIndex);

        // drop storage-attached index when a non-SAI exists
        createIndex("CREATE INDEX ON %s(v1)");
        dropIndex("DROP INDEX %s." + ndiIndex);
    }

    /**
     * Tests that storage-attached index queries can include restrictions over columns indexed by other indexes.
     */
    @Test
    public void shouldAcceptColumnsWithOtherIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");

        createIndex("CREATE INDEX ON %s(v1)");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v2) USING '%s'", StorageAttachedIndex.class.getName()));

        String insert = "INSERT INTO %s(k, v1, v2) VALUES (?, ?, ?)";
        execute(insert, 0, 0, 0);
        execute(insert, 1, 0, 1);
        execute(insert, 2, 1, 0);
        execute(insert, 3, 1, 1);

        String ossSelect = "SELECT * FROM %s WHERE v1 = ?";
        assertRowsIgnoringOrder(execute(ossSelect, 0), new Object[][]{{0, 0, 0}, {1, 0, 1}});
        assertRowsIgnoringOrder(execute(ossSelect, 1), new Object[][]{{2, 1, 0}, {3, 1, 1}});

        String saiSelect = "SELECT * FROM %s WHERE v1 = ? AND v2 = ? ALLOW FILTERING";
        assertRowsIgnoringOrder(execute(saiSelect, 0, 0), new Object[]{0, 0, 0});
        assertRowsIgnoringOrder(execute(saiSelect, 0, 1), new Object[]{1, 0, 1});
        assertRowsIgnoringOrder(execute(saiSelect, 1, 0), new Object[]{2, 1, 0});
        assertRowsIgnoringOrder(execute(saiSelect, 1, 1), new Object[]{3, 1, 1});
    }

    @Test
    public void shouldRequireAllowFilteringWithOtherIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k1 int, k2 int, " +
                    "s1 int static, " +
                    "c1 int, c2 int, c3 int, c4 int," +
                    "r1 int, r2 int, r3 int, " +
                    "PRIMARY KEY((k1, k2), c1, c2, c3, c4))");

        createIndex("CREATE INDEX ON %s(k1)");
        createIndex("CREATE INDEX ON %s(c4)");
        createIndex("CREATE INDEX ON %s(r3)");
        createIndex("CREATE INDEX ON %s(s1)");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c2) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c3) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(r1) USING '%s'", StorageAttachedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(r2) USING '%s'", StorageAttachedIndex.class.getName()));

        // without using the not-SAI index
        testAllowFiltering("SELECT * FROM %s", false);
        testAllowFiltering("SELECT * FROM %s WHERE c2=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE r1=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE r2=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c2=0 AND c3=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c2=0 AND r1=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c2=0 AND r2=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c3=0 AND r1=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c3=0 AND r2=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE r1=0 AND r2=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c2=0 AND c3=0 AND r1=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c2=0 AND c3=0 AND r2=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c2=0 AND r1=0 AND r2=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c3=0 AND r1=0 AND r2=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c2=0 AND c3=0 AND r1=0 AND r2=0", false);

        // using the not-SAI index on partition key
        testAllowFiltering("SELECT * FROM %s WHERE k1=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c2=0 AND c3=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c2=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c2=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c3=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c3=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c2=0 AND c3=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c2=0 AND c3=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c2=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c3=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE k1=0 AND c2=0 AND c3=0 AND r1=0 AND r2=0", true);

        // using the not-SAI index on clustering key
        testAllowFiltering("SELECT * FROM %s WHERE c4=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c2=0 AND c3=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c2=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c2=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c3=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c3=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c2=0 AND c3=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c2=0 AND c3=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c2=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c3=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE c4=0 AND c2=0 AND c3=0 AND r1=0 AND r2=0", true);

        // using the not-SAI index on regular column
        testAllowFiltering("SELECT * FROM %s WHERE r3=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c2=0 AND c3=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c2=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c2=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c3=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c3=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c2=0 AND c3=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c2=0 AND c3=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c2=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c3=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE r3=0 AND c2=0 AND c3=0 AND r1=0 AND r2=0", true);

        // using the not-SAI index on static column
        testAllowFiltering("SELECT * FROM %s WHERE s1=0", false);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c2=0 AND c3=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c2=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c2=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c3=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c3=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c2=0 AND c3=0 AND r1=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c2=0 AND c3=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c2=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c3=0 AND r1=0 AND r2=0", true);
        testAllowFiltering("SELECT * FROM %s WHERE s1=0 AND c2=0 AND c3=0 AND r1=0 AND r2=0", true);
    }

    private void testAllowFiltering(String query, boolean requiresAllowFiltering) throws Throwable
    {
        if (requiresAllowFiltering)
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, query);
        else
            assertNotNull(execute(query));

        assertNotNull(execute(query + " ALLOW FILTERING"));
    }
}
