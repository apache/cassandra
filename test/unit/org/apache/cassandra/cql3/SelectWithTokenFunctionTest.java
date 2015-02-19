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
        assertInvalid("SELECT * FROM %s WHERE token(a, b) >= token(?, ?)", "b", 0);
        assertInvalid("SELECT * FROM %s WHERE token(a) >= token(?) and token(a) >= token(?)", 0, 1);
        assertInvalid("SELECT * FROM %s WHERE token(a) >= token(?) and token(a) = token(?)", 0, 1);
        assertInvalidSyntax("SELECT * FROM %s WHERE token(a) = token(?) and token(a) IN (token(?))", 0, 1);
    }

    @Test
    public void testTokenFunctionWithPartitionKeyAndClusteringKeyArguments() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (a int, b text, PRIMARY KEY (a, b))");
        assertInvalid("SELECT * FROM %s WHERE token(a, b) > token(0, 'c')");
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
        assertInvalid("SELECT * FROM %s WHERE token(a) > token(?) and token(b) > token(?)", 0, "a");
        assertInvalid("SELECT * FROM %s WHERE token(a) > token(?, ?) and token(a) < token(?, ?) and token(b) > token(?, ?) ", 0, "a", 0, "d", 0, "a");
        assertInvalid("SELECT * FROM %s WHERE token(b, a) > token(0, 'c')");
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
