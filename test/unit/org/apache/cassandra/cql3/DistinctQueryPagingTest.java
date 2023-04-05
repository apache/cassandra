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
package org.apache.cassandra.cql3;

import org.junit.Assert;
import org.junit.Test;

public class DistinctQueryPagingTest extends CQLTester
{
    /**
     * Migrated from cql_tests.py:TestCQL.test_select_distinct()
     */
    @Test
    public void testSelectDistinct() throws Throwable
    {
        // Test a regular (CQL3) table.
        createTable("CREATE TABLE %s (pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY((pk0, pk1), ck0))");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 0, 0)", i, i);
            execute("INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 1, 1)", i, i);
        }

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 1"),
                   row(0, 0));

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 3"),
                   row(0, 0),
                   row(2, 2),
                   row(1, 1));

        // Test selection validation.
        assertInvalidMessage("queries must request all the partition key columns", "SELECT DISTINCT pk0 FROM %s");
        assertInvalidMessage("queries must only request partition key columns", "SELECT DISTINCT pk0, pk1, ck0 FROM %s");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.test_select_distinct_with_deletions()
     */
    @Test
    public void testSelectDistinctWithDeletions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c int, v int)");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, i, i);

        Object[][] rows = getRows(execute("SELECT DISTINCT k FROM %s"));
        Assert.assertEquals(10, rows.length);
        Object key_to_delete = rows[3][0];

        execute("DELETE FROM %s WHERE k=?", key_to_delete);

        rows = getRows(execute("SELECT DISTINCT k FROM %s"));
        Assert.assertEquals(9, rows.length);

        rows = getRows(execute("SELECT DISTINCT k FROM %s LIMIT 5"));
        Assert.assertEquals(5, rows.length);

        rows = getRows(execute("SELECT DISTINCT k FROM %s"));
        Assert.assertEquals(9, rows.length);
    }

    @Test
    public void testSelectDistinctWithWhereClause() throws Throwable {
        createTable("CREATE TABLE %s (k int, a int, b int, PRIMARY KEY (k, a))");
        createIndex("CREATE INDEX ON %s (b)");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i, i);
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i * 10, i * 10);
        }

        String distinctQueryErrorMsg = "SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.";
        assertInvalidMessage(distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE a >= 80 ALLOW FILTERING");

        assertInvalidMessage(distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE k IN (1, 2, 3) AND a = 10");

        assertInvalidMessage(distinctQueryErrorMsg,
                             "SELECT DISTINCT k FROM %s WHERE b = 5");

        assertRows(execute("SELECT DISTINCT k FROM %s WHERE k = 1"),
                   row(1));
        assertRows(execute("SELECT DISTINCT k FROM %s WHERE k IN (5, 6, 7)"),
                   row(5),
                   row(6),
                   row(7));

        // With static columns
        createTable("CREATE TABLE %s (k int, a int, s int static, b int, PRIMARY KEY (k, a))");
        createIndex("CREATE INDEX ON %s (b)");
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (k, a, b, s) VALUES (?, ?, ?, ?)", i, i, i, i);
            execute("INSERT INTO %s (k, a, b, s) VALUES (?, ?, ?, ?)", i, i * 10, i * 10, i * 10);
        }

        assertRows(execute("SELECT DISTINCT s FROM %s WHERE k = 5"),
                   row(50));
        assertRows(execute("SELECT DISTINCT s FROM %s WHERE k IN (5, 6, 7)"),
                   row(50),
                   row(60),
                   row(70));
    }

    @Test
    public void testSelectDistinctWithWhereClauseOnStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, a int, s int static, s1 int static, b int, PRIMARY KEY (k, a))");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", i, i, i, i, i);
            execute("INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", i, i * 10, i * 10, i * 10, i * 10);
        }

        execute("INSERT INTO %s (k, a, b, s, s1) VALUES (?, ?, ?, ?, ?)", 2, 10, 10, 10, 10);

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT DISTINCT k, s, s1 FROM %s WHERE s = 90 AND s1 = 90 ALLOW FILTERING"),
                       row(9, 90, 90));

            assertRows(execute("SELECT DISTINCT k, s, s1 FROM %s WHERE s = 90 AND s1 = 90 ALLOW FILTERING"),
                       row(9, 90, 90));

            assertRows(execute("SELECT DISTINCT k, s, s1 FROM %s WHERE s = 10 AND s1 = 10 ALLOW FILTERING"),
                       row(1, 10, 10),
                       row(2, 10, 10));

            assertRows(execute("SELECT DISTINCT k, s, s1 FROM %s WHERE k = 1 AND s = 10 AND s1 = 10 ALLOW FILTERING"),
                       row(1, 10, 10));
        });
    }

    @Test
    public void testSelectDistinctWithStaticColumnsAndPaging() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, s int static, c int, d int, primary key (a, b));");

        // Test with only static data
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (a, s) VALUES (?, ?)", i, i);

        testSelectDistinctWithPaging();

        // Test with a mix of partition with rows and partitions without rows
        for (int i = 0; i < 5; i++)
        {
            if (i % 2 == 0)
            {
                for (int j = 1; j < 4; j++)
                {
                    execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j);
                }
            }
        }

        testSelectDistinctWithPaging();

        // Test with all partition with rows
        for (int i = 0; i < 5; i++)
        {
            for (int j = 1; j < 4; j++)
            {
                execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", i, j, j, i + j);
            }
        }

        testSelectDistinctWithPaging();
    }

    private void testSelectDistinctWithPaging() throws Throwable
    {
        for (int pageSize = 1; pageSize < 7; pageSize++)
        {
            // Range query
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s FROM %s", pageSize),
                          row(1, 1),
                          row(0, 0),
                          row(2, 2),
                          row(4, 4),
                          row(3, 3));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s FROM %s LIMIT 3", pageSize),
                          row(1, 1),
                          row(0, 0),
                          row(2, 2));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s FROM %s WHERE s >= 2 ALLOW FILTERING", pageSize),
                          row(2, 2),
                          row(4, 4),
                          row(3, 3));

            // Multi partition query
            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s FROM %s WHERE a IN (1, 2, 3, 4);", pageSize),
                          row(1, 1),
                          row(2, 2),
                          row(3, 3),
                          row(4, 4));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s FROM %s WHERE a IN (1, 2, 3, 4) LIMIT 3;", pageSize),
                          row(1, 1),
                          row(2, 2),
                          row(3, 3));

            assertRowsNet(executeNetWithPaging("SELECT DISTINCT a, s FROM %s WHERE a IN (1, 2, 3, 4) AND s >= 2 ALLOW FILTERING;", pageSize),
                          row(2, 2),
                          row(3, 3),
                          row(4, 4));
        }
    }
}
