/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql3.validation.operations;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.dht.ByteOrderedPartitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * SELECT statement tests that require a ByteOrderedPartitioner
 */
public class SelectOrderedPartitionerTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    @Test
    public void testTokenAndIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        createIndex("CREATE INDEX ON %s(c)");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", i, i, i, i);
            execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", i, i + 10, i + 10, i + 10);
        }

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE token(a) > token(8) AND a = 9 AND c = 9 ALLOW FILTERING"),
                       row(9, 9, 9, 9));

            assertRows(execute("SELECT * FROM %s WHERE token(a) > token(8) AND a > 8 AND c = 9 ALLOW FILTERING"),
                       row(9, 9, 9, 9));
        });
    }

    @Test
    public void testFilteringOnAllPartitionKeysWithTokenRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", i, i, i, i);
            execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", i, i + 10, i + 10, i + 10);
        }

        assertEmpty(execute("SELECT * FROM %s WHERE token(a, b) > token(10, 10)"));
        assertEmpty(execute("SELECT * FROM %s WHERE token(a, b) > token(10, 10) AND a < 8 AND b < 8 ALLOW FILTERING"));
        assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(5, 5) AND a < 8 AND b < 8 ALLOW FILTERING"),
                   row(6, 6, 6, 6),
                   row(7, 7, 7, 7));
    }

    @Test
    public void testFilteringOnPartitionKeyWithToken() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))");
        createIndex("CREATE INDEX ON %s(d)");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", i, i, i, i);
            execute("INSERT INTO %s (a,b,c,d) VALUES (?, ?, ?, ?)", i, i + 10, i + 10, i + 10);
        }

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE token(a, b) > token(5, 10) AND b < 8 ALLOW FILTERING"),
                                    row(6, 6, 6, 6),
                                    row(7, 7, 7, 7));

            assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(8, 10) AND a = 9 ALLOW FILTERING"),
                       row(9, 9, 9, 9),
                       row(9, 19, 19, 19));

            assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(8, 10) AND a = 9 AND d = 9 ALLOW FILTERING"),
                       row(9, 9, 9, 9));

            assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(8, 10) AND a > 8 AND b > 8 AND d = 9 ALLOW FILTERING"),
                       row(9, 9, 9, 9));

            assertRows(execute("SELECT * FROM %s WHERE a = 9 AND b = 9  AND token(a, b) > token(8, 10) AND d = 9 ALLOW FILTERING"),
                       row(9, 9, 9, 9));

            assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(8, 10) AND a = 9 AND c = 19 ALLOW FILTERING"),
                       row(9, 19, 19, 19));

            assertEmpty(execute("SELECT * FROM %s WHERE token(a, b) = token(8, 8) AND b = 9 ALLOW FILTERING"));
        });
    }

    @Test
    public void testTokenAndCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (a frozen<map<int, int>>, b int, c int, PRIMARY KEY (a, b))");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (a,b,c) VALUES (?, ?, ?)", map(i, i), i, i);
            execute("INSERT INTO %s (a,b,c) VALUES (?, ?, ?)", map(i, i, 100, 200), i + 10, i + 10);
        }

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE token(a) > token({0: 0}) AND a CONTAINS KEY 9 ALLOW FILTERING"),
                                    row(map(9, 9), 9, 9),
                                    row(map(9, 9, 100, 200), 19, 19));

            assertRows(execute("SELECT * FROM %s WHERE token(a) > token({0: 0}) AND a CONTAINS KEY 9 AND a CONTAINS 200 ALLOW FILTERING"),
                       row(map(9, 9, 100, 200), 19, 19));

            assertRows(execute("SELECT * FROM %s WHERE token(a) > token({0: 0}) AND a CONTAINS KEY 9 AND b = 19 ALLOW FILTERING"),
                       row(map(9, 9, 100, 200), 19, 19));
        });
    }


    @Test
    public void testTokenFunctionWithSingleColumnPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (a int PRIMARY KEY, b text)");
        execute("INSERT INTO %s (a, b) VALUES (0, 'a')");

        assertRows(execute("SELECT * FROM %s WHERE token(a) >= token(?)", 0), row(0, "a"));
        assertRows(execute("SELECT * FROM %s WHERE token(a) >= token(?) and token(a) < token(?)", 0, 1), row(0, "a"));
        assertInvalid("SELECT * FROM %s WHERE token(a) > token(?)", "a");
        assertInvalidMessage("The token() function must contains only partition key components",
                             "SELECT * FROM %s WHERE token(a, b) >= token(?, ?)", "b", 0);
        assertInvalidMessage("More than one restriction was found for the start bound on a",
                             "SELECT * FROM %s WHERE token(a) >= token(?) and token(a) >= token(?)", 0, 1);
        assertInvalidMessage("Columns \"a\" cannot be restricted by both an equality and an inequality relation",
                             "SELECT * FROM %s WHERE token(a) >= token(?) and token(a) = token(?)", 0, 1);
        assertInvalidSyntax("SELECT * FROM %s WHERE token(a) = token(?) and token(a) IN (token(?))", 0, 1);

        assertInvalidMessage("More than one restriction was found for the start bound on a",
                             "SELECT * FROM %s WHERE token(a) > token(?) AND token(a) > token(?)", 1, 2);
        assertInvalidMessage("More than one restriction was found for the end bound on a",
                             "SELECT * FROM %s WHERE token(a) <= token(?) AND token(a) < token(?)", 1, 2);
        assertInvalidMessage("Columns \"a\" cannot be restricted by both an equality and an inequality relation",
                             "SELECT * FROM %s WHERE token(a) > token(?) AND token(a) = token(?)", 1, 2);
        assertInvalidMessage("a cannot be restricted by more than one relation if it includes an Equal",
                             "SELECT * FROM %s WHERE  token(a) = token(?) AND token(a) > token(?)", 1, 2);
    }

    @Test
    public void testTokenFunctionWithPartitionKeyAndClusteringKeyArguments() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (a int, b text, PRIMARY KEY (a, b))");
        assertInvalidMessage("The token() function must contains only partition key components",
                             "SELECT * FROM %s WHERE token(a, b) > token(0, 'c')");
    }

    @Test
    public void testTokenFunctionWithMultiColumnPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (a int, b text, PRIMARY KEY ((a, b)))");
        execute("INSERT INTO %s (a, b) VALUES (0, 'a')");
        execute("INSERT INTO %s (a, b) VALUES (0, 'b')");
        execute("INSERT INTO %s (a, b) VALUES (0, 'c')");

        assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(?, ?)", 0, "a"),
                   row(0, "b"),
                   row(0, "c"));
        assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(?, ?) and token(a, b) < token(?, ?)",
                           0, "a",
                           0, "d"),
                   row(0, "b"),
                   row(0, "c"));
        assertInvalidMessage("The token() function must be applied to all partition key components or none of them",
                             "SELECT * FROM %s WHERE token(a) > token(?) and token(b) > token(?)", 0, "a");
        assertInvalidMessage("The token() function must be applied to all partition key components or none of them",
                             "SELECT * FROM %s WHERE token(a) > token(?, ?) and token(a) < token(?, ?) and token(b) > token(?, ?) ",
                             0, "a", 0, "d", 0, "a");
        assertInvalidMessage("The token function arguments must be in the partition key order: a, b",
                             "SELECT * FROM %s WHERE token(b, a) > token(0, 'c')");
        assertInvalidMessage("The token() function must be applied to all partition key components or none of them",
                             "SELECT * FROM %s WHERE token(a, b) > token(?, ?) and token(b) < token(?, ?)", 0, "a", 0, "a");
        assertInvalidMessage("The token() function must be applied to all partition key components or none of them",
                             "SELECT * FROM %s WHERE token(a) > token(?, ?) and token(b) > token(?, ?)", 0, "a", 0, "a");
    }

    @Test
    public void testSingleColumnPartitionKeyWithTokenNonTokenRestrictionsMix() throws Throwable
    {
        createTable("CREATE TABLE %s (a int primary key, b int)");

        execute("INSERT INTO %s (a, b) VALUES (0, 0);");
        execute("INSERT INTO %s (a, b) VALUES (1, 1);");
        execute("INSERT INTO %s (a, b) VALUES (2, 2);");
        execute("INSERT INTO %s (a, b) VALUES (3, 3);");
        execute("INSERT INTO %s (a, b) VALUES (4, 4);");
        assertRows(execute("SELECT * FROM %s WHERE a IN (?, ?);", 1, 3),
                   row(1, 1),
                   row(3, 3));
        assertRows(execute("SELECT * FROM %s WHERE token(a)> token(?) and token(a) <= token(?);", 1, 3),
                   row(2, 2),
                   row(3, 3));
        assertRows(execute("SELECT * FROM %s WHERE token(a)= token(2);"),
                   row(2, 2));
        assertRows(execute("SELECT * FROM %s WHERE token(a) > token(?) AND token(a) <= token(?) AND a IN (?, ?);",
                           1, 3, 1, 3),
                   row(3, 3));
        assertRows(execute("SELECT * FROM %s WHERE token(a) < token(?) AND token(a) >= token(?) AND a IN (?, ?);",
                           1, 3, 1, 3),
                   row(3, 3));
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE token(a) > token(?) AND token(a) <= token(?) AND a > ?;", 1, 3, 1);

        assertRows(execute("SELECT * FROM %s WHERE token(a) > token(?) AND token(a) <= token(?) AND a IN ?;",
                           1, 3, Arrays.asList(1, 3)),
                   row(3, 3));
        assertRows(execute("SELECT * FROM %s WHERE token(a) > token(?) AND a = ?;", 1, 3),
                   row(3, 3));
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND token(a) > token(?);", 3, 1),
                   row(3, 3));
        assertEmpty(execute("SELECT * FROM %s WHERE token(a) > token(?) AND a = ?;", 3, 1));
        assertEmpty(execute("SELECT * FROM %s WHERE a = ? AND token(a) > token(?);", 1, 3));
        assertRows(execute("SELECT * FROM %s WHERE token(a) > token(?) AND a IN (?, ?);", 2, 1, 3),
                   row(3, 3));
        assertRows(execute("SELECT * FROM %s WHERE token(a) > token(?) AND token(a) < token(?) AND a IN (?, ?) ;", 2, 5, 1, 3),
                   row(3, 3));
        assertRows(execute("SELECT * FROM %s WHERE a IN (?, ?) AND token(a) > token(?) AND token(a) < token(?);", 1, 3, 2, 5),
                   row(3, 3));
        assertRows(execute("SELECT * FROM %s WHERE token(a) > token(?) AND a IN (?, ?) AND token(a) < token(?);", 2, 1, 3, 5),
                   row(3, 3));
        assertEmpty(execute("SELECT * FROM %s WHERE a IN (?, ?) AND token(a) > token(?);", 1, 3, 3));
        assertRows(execute("SELECT * FROM %s WHERE token(a) <= token(?) AND a = ?;", 2, 2),
                   row(2, 2));
        assertEmpty(execute("SELECT * FROM %s WHERE token(a) <= token(?) AND a = ?;", 2, 3));
        assertEmpty(execute("SELECT * FROM %s WHERE token(a) = token(?) AND a = ?;", 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE token(a) >= token(?) AND token(a) <= token(?) AND a = ?;", 2, 2, 2),
                   row(2, 2));
        assertEmpty(execute("SELECT * FROM %s WHERE token(a) >= token(?) AND token(a) < token(?) AND a = ?;", 2, 2, 2));
        assertEmpty(execute("SELECT * FROM %s WHERE token(a) > token(?) AND token(a) <= token(?) AND a = ?;", 2, 2, 2));
        assertEmpty(execute("SELECT * FROM %s WHERE token(a) > token(?) AND token(a) < token(?) AND a = ?;", 2, 2, 2));
    }

    @Test
    public void testMultiColumnPartitionKeyWithTokenNonTokenRestrictionsMix() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, primary key((a, b)))");

        execute("INSERT INTO %s (a, b, c) VALUES (0, 0, 0);");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 1);");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 2, 2);");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 0, 3);");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 4);");

        assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(?, ?);", 0, 0),
                   row(0, 1, 1),
                   row(0, 2, 2),
                   row(1, 0, 3),
                   row(1, 1, 4));

        assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(?, ?) AND a = ? AND b IN (?, ?);",
                           0, 0, 1, 0, 1),
                   row(1, 0, 3),
                   row(1, 1, 4));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND token(a, b) > token(?, ?) AND b IN (?, ?);",
                           1, 0, 0, 0, 1),
                   row(1, 0, 3),
                   row(1, 1, 4));

        assertRows(execute("SELECT * FROM %s WHERE b IN (?, ?) AND token(a, b) > token(?, ?) AND a = ?;",
                           0, 1, 0, 0, 1),
                   row(1, 0, 3),
                   row(1, 1, 4));

        assertEmpty(execute("SELECT * FROM %s WHERE b IN (?, ?) AND token(a, b) > token(?, ?) AND token(a, b) < token(?, ?) AND a = ?;",
                            0, 1, 0, 0, 0, 0, 1));

        assertEmpty(execute("SELECT * FROM %s WHERE b IN (?, ?) AND token(a, b) > token(?, ?) AND token(a, b) <= token(?, ?) AND a = ?;",
                            0, 1, 0, 0, 0, 0, 1));

        assertEmpty(execute("SELECT * FROM %s WHERE b IN (?, ?) AND token(a, b) >= token(?, ?) AND token(a, b) < token(?, ?) AND a = ?;",
                            0, 1, 0, 0, 0, 0, 1));

        assertEmpty(execute("SELECT * FROM %s WHERE b IN (?, ?) AND token(a, b) = token(?, ?) AND a = ?;",
                            0, 1, 0, 0, 1));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE token(a, b) > token(?, ?) AND a = ?;", 0, 0, 1);
    }

    @Test
    public void testMultiColumnPartitionKeyWithIndexAndTokenNonTokenRestrictionsMix() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, primary key((a, b)))");
        createIndex("CREATE INDEX ON %s(b)");
        createIndex("CREATE INDEX ON %s(c)");

        execute("INSERT INTO %s (a, b, c) VALUES (0, 0, 0);");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 1);");
        execute("INSERT INTO %s (a, b, c) VALUES (0, 2, 2);");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 0, 3);");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 4);");

        assertRows(execute("SELECT * FROM %s WHERE b = ?;", 1),
                   row(0, 1, 1),
                   row(1, 1, 4));

        assertRows(execute("SELECT * FROM %s WHERE token(a, b) > token(?, ?) AND b = ?;", 0, 0, 1),
                   row(0, 1, 1),
                   row(1, 1, 4));

        assertRows(execute("SELECT * FROM %s WHERE b = ? AND token(a, b) > token(?, ?);", 1, 0, 0),
                   row(0, 1, 1),
                   row(1, 1, 4));

        assertRows(execute("SELECT * FROM %s WHERE b = ? AND token(a, b) > token(?, ?) and c = ? ALLOW FILTERING;", 1, 0, 0, 4),
                   row(1, 1, 4));
    }

    @Test
    public void testTokenFunctionWithCompoundPartitionAndClusteringCols() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c, d))");
        // just test that the queries don't error
        execute("SELECT * FROM %s WHERE token(a, b) > token(0, 0) AND c > 10 ALLOW FILTERING;");
        execute("SELECT * FROM %s WHERE c > 10 AND token(a, b) > token(0, 0) ALLOW FILTERING;");
        execute("SELECT * FROM %s WHERE token(a, b) > token(0, 0) AND (c, d) > (0, 0) ALLOW FILTERING;");
        execute("SELECT * FROM %s WHERE (c, d) > (0, 0) AND token(a, b) > token(0, 0) ALLOW FILTERING;");
    }

    /**
     * Test undefined columns
     * migrated from cql_tests.py:TestCQL.undefined_column_handling_test()
     */
    @Test
    public void testUndefinedColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int,)");

        execute("INSERT INTO %s (k, v1, v2) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (k, v1) VALUES (1, 1)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (2, 2, 2)");

        Object[][] rows = getRows(execute("SELECT v2 FROM %s"));
        assertEquals(0, rows[0][0]);
        assertNull(rows[1][0]);
        assertEquals(2, rows[2][0]);

        rows = getRows(execute("SELECT v2 FROM %s WHERE k = 1"));
        assertEquals(1, rows.length);
        assertNull(rows[0][0]);
    }

    /**
     * Check table with only a PK (#4361),
     * migrated from cql_tests.py:TestCQL.only_pk_test()
     */
    @Test
    public void testPrimaryKeyOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, PRIMARY KEY (k, c))");

        for (int k = 0; k < 2; k++)
            for (int c = 0; c < 2; c++)
                execute("INSERT INTO %s (k, c) VALUES (?, ?)", k, c);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0),
                   row(0, 1),
                   row(1, 0),
                   row(1, 1));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.composite_index_with_pk_test()
     */
    @Test
    public void testCompositeIndexWithPK() throws Throwable
    {
        createTable("CREATE TABLE %s (blog_id int, time1 int, time2 int, author text, content text, PRIMARY KEY (blog_id, time1, time2))");

        createIndex("CREATE INDEX ON %s(author)");

        execute("INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)", 1, 0, 0, "foo", "bar1");
        execute("INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)", 1, 0, 1, "foo", "bar2");
        execute("INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)", 2, 1, 0, "foo", "baz");
        execute("INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)", 3, 0, 1, "gux", "qux");

        assertRows(execute("SELECT blog_id, content FROM %s WHERE author='foo'"),
                   row(1, "bar1"),
                   row(1, "bar2"),
                   row(2, "baz"));

        assertRows(execute("SELECT blog_id, content FROM %s WHERE time1 > 0 AND author='foo' ALLOW FILTERING"),
                   row(2, "baz"));

        assertRows(execute("SELECT blog_id, content FROM %s WHERE time1 = 1 AND author='foo' ALLOW FILTERING"),
                   row(2, "baz"));

        assertRows(execute("SELECT blog_id, content FROM %s WHERE time1 = 1 AND time2 = 0 AND author='foo' ALLOW FILTERING"),
                   row(2, "baz"));

        assertEmpty(execute("SELECT content FROM %s WHERE time1 = 1 AND time2 = 1 AND author='foo' ALLOW FILTERING"));

        assertEmpty(execute("SELECT content FROM %s WHERE time1 = 1 AND time2 > 0 AND author='foo' ALLOW FILTERING"));

        assertInvalid("SELECT content FROM %s WHERE time2 >= 0 AND author='foo'");

        assertInvalid("SELECT blog_id, content FROM %s WHERE time1 > 0 AND author='foo'");
        assertInvalid("SELECT blog_id, content FROM %s WHERE time1 = 1 AND author='foo'");
        assertInvalid("SELECT blog_id, content FROM %s WHERE time1 = 1 AND time2 = 0 AND author='foo'");
        assertInvalid("SELECT content FROM %s WHERE time1 = 1 AND time2 = 1 AND author='foo'");
        assertInvalid("SELECT content FROM %s WHERE time1 = 1 AND time2 > 0 AND author='foo'");
    }

    /**
     * Test for LIMIT bugs from 4579,
     * migrated from cql_tests.py:TestCQL.limit_bugs_test()
     */
    @Test
    public void testLimitBug() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c, d, e) VALUES (1, 1, 1, 1, 1);");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (2, 2, 2, 2, 2);");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (3, 3, 3, 3, 3);");
        execute("INSERT INTO %s (a, b, c, d, e) VALUES (4, 4, 4, 4, 4);");

        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, 1, 1, 1),
                   row(2, 2, 2, 2, 2),
                   row(3, 3, 3, 3, 3),
                   row(4, 4, 4, 4, 4));

        assertRows(execute("SELECT * FROM %s LIMIT 1"),
                   row(1, 1, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s LIMIT 2"),
                   row(1, 1, 1, 1, 1),
                   row(2, 2, 2, 2, 2));

        createTable("CREATE TABLE %s (a int primary key, b int, c int,)");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 2, 2)");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 3, 3)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 4, 4)");

        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, 1),
                   row(2, 2, 2),
                   row(3, 3, 3),
                   row(4, 4, 4));

        assertRows(execute("SELECT * FROM %s LIMIT 1"),
                   row(1, 1, 1));

        assertRows(execute("SELECT * FROM %s LIMIT 2"),
                   row(1, 1, 1),
                   row(2, 2, 2));

        assertRows(execute("SELECT * FROM %s LIMIT 3"),
                   row(1, 1, 1),
                   row(2, 2, 2),
                   row(3, 3, 3));

        assertRows(execute("SELECT * FROM %s LIMIT 4"),
                   row(1, 1, 1),
                   row(2, 2, 2),
                   row(3, 3, 3),
                   row(4, 4, 4));

        assertRows(execute("SELECT * FROM %s LIMIT 5"),
                   row(1, 1, 1),
                   row(2, 2, 2),
                   row(3, 3, 3),
                   row(4, 4, 4));
    }

    /**
     * Test for #4612 bug and more generally order by when multiple C* rows are queried
     * migrated from cql_tests.py:TestCQL.order_by_multikey_test()
     */
    @Test
    public void testOrderByMultikey() throws Throwable
    {
        createTable("CREATE TABLE %s (my_id varchar, col1 int, col2 int, value varchar, PRIMARY KEY (my_id, col1, col2))");

        execute("INSERT INTO %s (my_id, col1, col2, value) VALUES ( 'key1', 1, 1, 'a');");
        execute("INSERT INTO %s (my_id, col1, col2, value) VALUES ( 'key2', 3, 3, 'a');");
        execute("INSERT INTO %s (my_id, col1, col2, value) VALUES ( 'key3', 2, 2, 'b');");
        execute("INSERT INTO %s (my_id, col1, col2, value) VALUES ( 'key4', 2, 1, 'b');");

        assertRows(execute("SELECT col1 FROM %s WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                   row(1), row(2), row(3));

        assertRows(execute("SELECT col1, value, my_id, col2 FROM %s WHERE my_id in('key3', 'key4') ORDER BY col1, col2"),
                   row(2, "b", "key4", 1), row(2, "b", "key3", 2));

        assertInvalid("SELECT col1 FROM %s ORDER BY col1");
        assertInvalid("SELECT col1 FROM %s WHERE my_id > 'key1' ORDER BY col1");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.composite_index_collections_test()
     */
    @Test
    public void testIndexOnCompositeWithCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (blog_id int, time1 int, time2 int, author text, content set<text>, PRIMARY KEY (blog_id, time1, time2))");

        createIndex("CREATE INDEX ON %s (author)");

        execute("INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, { 'bar1', 'bar2' })", 1, 0, 0, "foo");
        execute("INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, { 'bar2', 'bar3' })", 1, 0, 1, "foo");
        execute("INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, { 'baz' })", 2, 1, 0, "foo");
        execute("INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, { 'qux' })", 3, 0, 1, "gux");

        assertRows(execute("SELECT blog_id, content FROM %s WHERE author='foo'"),
                   row(1, set("bar1", "bar2")),
                   row(1, set("bar2", "bar3")),
                   row(2, set("baz")));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.truncate_clean_cache_test()
     */
    @Test
    public void testTruncateWithCaching() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int) WITH CACHING = { 'keys': 'ALL', 'rows_per_partition': 'ALL' };");

        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)", i, i, i * 2);

        assertRows(execute("SELECT v1, v2 FROM %s WHERE k IN (0, 1, 2)"),
                   row(0, 0),
                   row(1, 2),
                   row(2, 4));

        execute("TRUNCATE %s");

        assertEmpty(execute("SELECT v1, v2 FROM %s WHERE k IN (0, 1, 2)"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.range_key_ordered_test()
     */
    @Test
    public void testRangeKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");

        execute("INSERT INTO %s (k) VALUES (-1)");
        execute("INSERT INTO %s (k) VALUES ( 0)");
        execute("INSERT INTO %s (k) VALUES ( 1)");

        assertRows(execute("SELECT * FROM %s"),
                   row(0),
                   row(1),
                   row(-1));

        assertInvalid("SELECT * FROM %s WHERE k >= -1 AND k < 1");
    }

    @Test
    public void testTokenFunctionWithInvalidColumnNames() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))");
        assertInvalidMessage("Undefined column name e", "SELECT * FROM %s WHERE token(a, e) = token(0, 0)");
        assertInvalidMessage("Undefined column name e", "SELECT * FROM %s WHERE token(a, e) > token(0, 1)");
        assertInvalidMessage("Undefined column name e", "SELECT b AS e FROM %s WHERE token(a, e) = token(0, 0)");
        assertInvalidMessage("Undefined column name e", "SELECT b AS e FROM %s WHERE token(a, e) > token(0, 1)");
    }
}
