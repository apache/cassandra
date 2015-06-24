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
package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertTrue;

public class SelectOrderByTest extends CQLTester
{
    @Test
    public void testNormalSelectionOrderSingleClustering() throws Throwable
    {
        for (String descOption : new String[]{"", " WITH CLUSTERING ORDER BY (b DESC)"})
        {
            createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))" + descOption);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 2);

            assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0, 0, 0),
                    row(0, 1, 1),
                    row(0, 2, 2)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(0, 2, 2),
                    row(0, 1, 1),
                    row(0, 0, 0)
            );

            // order by the only column in the selection
            assertRows(execute("SELECT b FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT b FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));

            // order by a column not in the selection
            assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));
        }
    }

    @Test
    public void testFunctionSelectionOrderSingleClustering() throws Throwable
    {
        for (String descOption : new String[]{"", " WITH CLUSTERING ORDER BY (b DESC)"})
        {
            createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))" + descOption);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 2);

            // order by the only column in the selection
            assertRows(execute("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));

            // order by a column not in the selection
            assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));

            assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY c ASC", 0);
            assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY c DESC", 0);
        }
    }

    @Test
    public void testFieldSelectionOrderSingleClustering() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");

        for (String descOption : new String[]{"", " WITH CLUSTERING ORDER BY (b DESC)"})
        {
            createTable("CREATE TABLE %s (a int, b int, c frozen<" + type + "   >, PRIMARY KEY (a, b))" + descOption);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, {a: ?})", 0, 0, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, {a: ?})", 0, 1, 1);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, {a: ?})", 0, 2, 2);

            // order by a column not in the selection
            assertRows(execute("SELECT c.a FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT c.a FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));

            assertRows(execute("SELECT blobAsInt(intAsBlob(c.a)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                       row(2), row(1), row(0));
            dropTable("DROP TABLE %s");
        }
    }

    @Test
    public void testNormalSelectionOrderMultipleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 2, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 3);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 4);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 2, 5);

        assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0, 0, 0, 0),
                row(0, 0, 1, 1),
                row(0, 0, 2, 2),
                row(0, 1, 0, 3),
                row(0, 1, 1, 4),
                row(0, 1, 2, 5)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(0, 1, 2, 5),
                row(0, 1, 1, 4),
                row(0, 1, 0, 3),
                row(0, 0, 2, 2),
                row(0, 0, 1, 1),
                row(0, 0, 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(0, 1, 2, 5),
                row(0, 1, 1, 4),
                row(0, 1, 0, 3),
                row(0, 0, 2, 2),
                row(0, 0, 1, 1),
                row(0, 0, 0, 0)
        );

        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY c ASC", 0);
        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY c DESC", 0);
        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY b ASC, c DESC", 0);
        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY b DESC, c ASC", 0);
        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY d ASC", 0);

        // select and order by b
        assertRows(execute("SELECT b FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0), row(0), row(0), row(1), row(1), row(1));
        assertRows(execute("SELECT b FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(1), row(1), row(1), row(0), row(0), row(0));

        // select c, order by b
        assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0), row(1), row(2), row(0), row(1), row(2));
        assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(2), row(1), row(0), row(2), row(1), row(0));

        // select c, order by b, c
        assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                row(0), row(1), row(2), row(0), row(1), row(2));
        assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(2), row(1), row(0), row(2), row(1), row(0));

        // select d, order by b, c
        assertRows(execute("SELECT d FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                row(0), row(1), row(2), row(3), row(4), row(5));
        assertRows(execute("SELECT d FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(5), row(4), row(3), row(2), row(1), row(0));
    }

    @Test
    public void testFunctionSelectionOrderMultipleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 2, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 3);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 4);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 2, 5);

        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY c ASC", 0);
        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY c DESC", 0);
        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC, c DESC", 0);
        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC, c ASC", 0);
        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY d ASC", 0);

        // select and order by b
        assertRows(execute("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                   row(0), row(0), row(0), row(1), row(1), row(1));
        assertRows(execute("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                   row(1), row(1), row(1), row(0), row(0), row(0));

        assertRows(execute("SELECT b, blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0, 0), row(0, 0), row(0, 0), row(1, 1), row(1, 1), row(1, 1));
        assertRows(execute("SELECT b, blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(1, 1), row(1, 1), row(1, 1), row(0, 0), row(0, 0), row(0, 0));

        // select c, order by b
        assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0), row(1), row(2), row(0), row(1), row(2));
        assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(2), row(1), row(0), row(2), row(1), row(0));

        // select c, order by b, c
        assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                row(0), row(1), row(2), row(0), row(1), row(2));
        assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(2), row(1), row(0), row(2), row(1), row(0));

        // select d, order by b, c
        assertRows(execute("SELECT blobAsInt(intAsBlob(d)) FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                row(0), row(1), row(2), row(3), row(4), row(5));
        assertRows(execute("SELECT blobAsInt(intAsBlob(d)) FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(5), row(4), row(3), row(2), row(1), row(0));

    }

    /**
     * Check ORDER BY support in SELECT statement
     * migrated from cql_tests.py:TestCQL.order_by_test()
     */
    @Test
    public void testSimpleOrderBy() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRows(execute("SELECT v FROM %s WHERE k = 0 ORDER BY c DESC"),
                   row(9), row(8), row(7), row(6), row(5), row(4), row(3), row(2), row(1), row(0));

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)) WITH COMPACT STORAGE");

        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 2; j++)
                execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, ?, ?, ?)", i, j, i * 2 + j);

        assertInvalid("SELECT v FROM %s WHERE k = 0 ORDER BY c DESC");
        assertInvalid("SELECT v FROM %s WHERE k = 0 ORDER BY c2 DESC");
        assertInvalid("SELECT v FROM %s WHERE k = 0 ORDER BY k DESC");

        assertRows(execute("SELECT v FROM %s WHERE k = 0 ORDER BY c1 DESC"),
                   row(7), row(6), row(5), row(4), row(3), row(2), row(1), row(0));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 ORDER BY c1"),
                   row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7));
    }

    /**
     * More ORDER BY checks (#4160)
     * migrated from cql_tests.py:TestCQL.more_order_by_test()
     */
    @Test
    public void testMoreOrderBy() throws Throwable
    {
        createTable("CREATE TABLE %s (row text, number int, string text, PRIMARY KEY(row, number)) WITH COMPACT STORAGE ");

        execute("INSERT INTO %s (row, number, string) VALUES ('row', 1, 'one')");
        execute("INSERT INTO %s (row, number, string) VALUES ('row', 2, 'two')");
        execute("INSERT INTO %s (row, number, string) VALUES ('row', 3, 'three')");
        execute("INSERT INTO %s (row, number, string) VALUES ('row', 4, 'four')");

        assertRows(execute("SELECT number FROM %s WHERE row='row' AND number < 3 ORDER BY number ASC"),
                   row(1), row(2));

        assertRows(execute("SELECT number FROM %s WHERE row='row' AND number >= 3 ORDER BY number ASC"),
                   row(3), row(4));

        assertRows(execute("SELECT number FROM %s WHERE row='row' AND number < 3 ORDER BY number DESC"),
                   row(2), row(1));

        assertRows(execute("SELECT number FROM %s WHERE row='row' AND number >= 3 ORDER BY number DESC"),
                   row(4), row(3));

        assertRows(execute("SELECT number FROM %s WHERE row='row' AND number > 3 ORDER BY number DESC"),
                   row(4));

        assertRows(execute("SELECT number FROM %s WHERE row='row' AND number <= 3 ORDER BY number DESC"),
                   row(3), row(2), row(1));
    }

    /**
     * Check we don't allow order by on row key (#4246)
     * migrated from cql_tests.py:TestCQL.order_by_validation_test()
     */
    @Test
    public void testInvalidOrderBy() throws Throwable
    {
        createTable("CREATE TABLE %s( k1 int, k2 int, v int, PRIMARY KEY (k1, k2))");

        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 2, 2, 2);

        assertInvalid("SELECT * FROM %s ORDER BY k2");
    }

    /**
     * Check that order-by works with IN (#4327)
     * migrated from cql_tests.py:TestCQL.order_by_with_in_test()
     */
    @Test
    public void testOrderByForInClause() throws Throwable
    {
        createTable("CREATE TABLE %s (my_id varchar, col1 int, value varchar, PRIMARY KEY (my_id, col1))");

        execute("INSERT INTO %s (my_id, col1, value) VALUES ( 'key1', 1, 'a')");
        execute("INSERT INTO %s (my_id, col1, value) VALUES ( 'key2', 3, 'c')");
        execute("INSERT INTO %s (my_id, col1, value) VALUES ( 'key3', 2, 'b')");
        execute("INSERT INTO %s (my_id, col1, value) VALUES ( 'key4', 4, 'd')");

        assertRows(execute("SELECT col1 FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                   row(1), row(2), row(3));

        assertRows(execute("SELECT col1, my_id FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                   row(1, "key1"), row(2, "key3"), row(3, "key2"));

        assertRows(execute("SELECT my_id, col1 FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                   row("key1", 1), row("key3", 2), row("key2", 3));
    }

    /**
     * Test reversed comparators
     * migrated from cql_tests.py:TestCQL.reversed_comparator_test()
     */
    @Test
    public void testReversedComparator() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC);");

        for(int i =0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRows(execute("SELECT c, v FROM %s WHERE k = 0 ORDER BY c ASC"),
                   row(0, 0), row(1, 1), row(2, 2), row(3, 3), row(4, 4),
                   row(5, 5), row(6, 6), row(7, 7), row(8, 8), row(9, 9));

        assertRows(execute("SELECT c, v FROM %s WHERE k = 0 ORDER BY c DESC"),
                   row(9, 9), row(8, 8), row(7, 7), row(6, 6), row(5, 5),
                   row(4, 4), row(3, 3), row(2, 2), row(1, 1), row(0, 0));

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v text, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");

        for(int i = 0; i < 10; i++)
            for(int j = 0; j < 10; j++)
                execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, ?, ?, ?)", i, j, String.format("%d%d", i, j));

        assertInvalid("SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 ASC");
        assertInvalid("SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 DESC, c2 DESC");

        Object[][] expectedRows = new Object[100][];
        for(int i = 0; i < 10; i++)
            for(int j = 9; j >= 0; j--)
                expectedRows[i * 10 + (9 - j)] = row(i, j, String.format("%d%d", i, j));

        assertRows(execute("SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 ASC"),
                   expectedRows);

        assertRows(execute("SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 DESC"),
                   expectedRows);

        for(int i = 9; i >= 0; i--)
            for(int j = 0; j < 10; j++)
                expectedRows[(9 - i) * 10 + j] = row(i, j, String.format("%d%d", i, j));

        assertRows(execute("SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c1 DESC, c2 ASC"),
                   expectedRows);

        assertInvalid("SELECT c1, c2, v FROM %s WHERE k = 0 ORDER BY c2 DESC, c1 ASC");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multiordering_test()
     */
    @Test
    public void testMultiordering() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c1 int, c2 int, PRIMARY KEY (k, c1, c2) ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");

        for (int i = 0; i < 2; i++)
            for (int j = 0; j < 2; j++)
                execute("INSERT INTO %s (k, c1, c2) VALUES ('foo', ?, ?)", i, j);

        assertRows(execute("SELECT c1, c2 FROM %s WHERE k = 'foo'"),
                   row(0, 1), row(0, 0), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c1 ASC, c2 DESC"),
                   row(0, 1), row(0, 0), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c1 DESC, c2 ASC"),
                   row(1, 0), row(1, 1), row(0, 0), row(0, 1));

        assertInvalid("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c2 DESC");
        assertInvalid("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c2 ASC");
        assertInvalid("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c1 ASC, c2 ASC");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.in_with_desc_order_test()
     */
    @Test
    public void testSelectInStatementWithDesc() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2))");
        execute("INSERT INTO %s(k, c1, c2) VALUES (0, 0, 0)");
        execute("INSERT INTO %s(k, c1, c2) VALUES (0, 0, 1)");
        execute("INSERT INTO %s(k, c1, c2) VALUES (0, 0, 2)");

        assertRows(execute("SELECT * FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC"),
                   row(0, 0, 2),
                   row(0, 0, 0));
    }

    /**
     * Test that columns don't need to be selected for ORDER BY when there is a IN (#4911),
     * migrated from cql_tests.py:TestCQL.in_order_by_without_selecting_test()
     */
    @Test
    public void testInOrderByWithoutSelecting() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))");

        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 0, 3)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 1, 4)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 2, 5)");

        assertRows(execute("SELECT * FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)"),
                   row(0, 0, 0, 0),
                   row(0, 0, 2, 2));
        assertRows(execute("SELECT * FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0, 0, 0),
                   row(0, 0, 2, 2));

        // check that we don 't need to select the column on which we order
        assertRows(execute("SELECT v FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)"),
                   row(0),
                   row(2));
        assertRows(execute("SELECT v FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC"),
                   row(0),
                   row(2));
        assertRows(execute("SELECT v FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC"),
                   row(2),
                   row(0));
        assertRows(execute("SELECT v FROM %s WHERE k IN (1, 0)"),
                   row(3),
                   row(4),
                   row(5),
                   row(0),
                   row(1),
                   row(2));

        assertRows(execute("SELECT v FROM %s WHERE k IN (1, 0) ORDER BY c1 ASC"),
                   row(0),
                   row(1),
                   row(2),
                   row(3),
                   row(4),
                   row(5));

        // we should also be able to use functions in the select clause (additional test for CASSANDRA - 8286)
        Object[][] results = getRows(execute("SELECT writetime(v) FROM %s WHERE k IN (1, 0) ORDER BY c1 ASC"));

        // since we don 't know the write times, just assert that the order matches the order we expect
        assertTrue(isFirstIntSorted(results));
    }

    private boolean isFirstIntSorted(Object[][] rows)
    {
        for (int i = 1; i < rows.length; i++)
        {
            Long prev = (Long)rows[i-1][0];
            Long curr = (Long)rows[i][0];

            if (prev > curr)
                return false;
        }

        return true;
    }
}
