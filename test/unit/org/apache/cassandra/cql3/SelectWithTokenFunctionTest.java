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

import java.util.Arrays;

import org.junit.Test;

public class SelectWithTokenFunctionTest extends CQLTester
{
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
        assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
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

        assertInvalidMessage("Partition key parts: b must be restricted as other parts are",
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
}
